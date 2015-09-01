import { expect } from "chai";
import sinon from "sinon";

import { PushWorker } from "../src/index";
import { CREATE_CONTEXT, MQ_CONTEXT } from "../src/symbols";

describe("Time Service PushWorker Test", () => {

  function generateTestResources(eventName, deadLetterQueue, options = {}) {
    let workerData = options.workerData || {};
    let lastRetry = options.lastRetry || false;
    let onStub = sinon.stub();
    onStub.withArgs("ready").yields([]);

    // stub out a worker connetion
    let workerConnectionStub = sinon.stub();
    workerConnectionStub.withArgs(eventName).yields();

    let workerOnStub = sinon.stub();
    let workerMessage = {
      task: workerData,
      retries: lastRetry ? 1 : 10,
      retryWait: 0
    };
    workerOnStub.withArgs("data").yields([JSON.stringify(workerMessage)]);

    // stub out removeAllListener
    let removeAllListenersWorkerStub = sinon.stub();

    // stub out the PUSH connect object
    let pushConnectionStub = sinon.stub();
    pushConnectionStub.withArgs(eventName).yields();

    pushConnectionStub.withArgs(deadLetterQueue).yields();

    // stub out the PUSH write object
    let pushWriteStub = sinon.stub();

    let closeStub = sinon.stub();

    let ackStub = sinon.stub();

    // stub out the socket
    let socketStub = sinon.stub();
    let workerOptions = {prefetch: 1, persistant: true};
    socketStub.withArgs("WORKER", workerOptions).returns({
      connect: workerConnectionStub,
      on: workerOnStub,
      removeAllListeners: removeAllListenersWorkerStub,
      ack: ackStub,
      close: closeStub
    });

    // push socket
    socketStub.withArgs("PUSH").returns({
      connect: pushConnectionStub,
      write: pushWriteStub,
      close: closeStub
    });

    // create rabbit.js context
    let mqContext = {
      on: onStub,
      socket: socketStub
    };

    sinon.stub(PushWorker.prototype, CREATE_CONTEXT, () => mqContext);
    let pushworker = new PushWorker(
      eventName,
      deadLetterQueue,
      "amqp://somefakeserver"
    );

    return {
      pushworker,
      mqContext,
      onStub,
      workerConnectionStub,
      workerOnStub,
      pushConnectionStub,
      pushWriteStub,
      socketStub,
      closeStub,
      ackStub,
      removeAllListenersWorkerStub
    };
  }

  afterEach(() => {
    PushWorker.prototype[CREATE_CONTEXT].restore();
  });

  it("does have an event name and context", () => {
    let { pushworker } = generateTestResources("task", "dead");
    expect(pushworker.queue).to.equal("task");
    expect(pushworker[MQ_CONTEXT]).to.not.equal(undefined);
    expect(pushworker.connected).to.equal(false);
  });

  it("does connect to push and worker streams", (done) => {
    let {
      pushworker,
      onStub,
      pushConnectionStub,
      workerConnectionStub,
      socketStub
    } = generateTestResources("task", "dead");

    pushworker.connect().then(() => {
      // PUSH and WORKER sockets should be connected
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      expect(pushworker.connected).to.equal(true);
      done();
    }).catch((error) => done(error));
  });

  it("does push to the task stream", (done) => {
    let task = {bananas: 100};
    let {
      pushworker,
      onStub,
      pushConnectionStub,
      workerConnectionStub,
      pushWriteStub,
      socketStub
    } = generateTestResources("eat", "dead");

    pushworker.connect()
      .then(() => pushworker.push(task))
      .then(() => {
        // PUSH and WORKER sockets should be connected
        expect(socketStub.callCount).to.equal(3);
        expect(pushConnectionStub.callCount).to.equal(2);
        expect(workerConnectionStub.callCount).to.equal(1);
        expect(onStub.callCount).to.equal(1);
        expect(pushWriteStub.callCount).to.equal(1);
        expect(pushworker.connected).to.equal(true);
        expect(pushWriteStub.callCount).to.equal(1);

        let expectedPushMessage = [JSON.stringify({
          task: task,
          retries: 10,
          retryWait: 10
        }), "utf8"];

        expect(pushWriteStub.lastCall.args).to.eql(expectedPushMessage);
        done();
      })
      .catch((error) => done(error));
  });

  it("does subscribe to a task stream", (done) => {
    let workerData = {doStuff: true};
    let {
      pushworker,
      onStub,
      workerOnStub,
      pushConnectionStub,
      workerConnectionStub,
      socketStub
    } = generateTestResources("task", "dead", {workerData: workerData});

    pushworker.on("task", (task) => {
      // PUSH and WORKER sockets should be connected
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      expect(workerOnStub.callCount).to.equal(1);
      expect(pushworker.connected).to.equal(true);
      expect(task.task).to.eql(workerData);
      done();
    });

    pushworker.connect()
      .then(() => pushworker.subscribe())
      .catch((error) => done(error));
  });

  it("does unsubscribe from a task stream", (done) => {
    let {
      pushworker,
      onStub,
      workerOnStub,
      pushConnectionStub,
      workerConnectionStub,
      socketStub,
      removeAllListenersWorkerStub
    } = generateTestResources("task", "dead");

    pushworker.connect()
      .then(() => pushworker.subscribe())
      .then(() => pushworker.unsubscribe())
      .then(() => {
        // PUSH and WORKER sockets should have been connected once each
        expect(socketStub.callCount).to.equal(3);
        expect(pushConnectionStub.callCount).to.equal(2);
        expect(workerConnectionStub.callCount).to.equal(1);
        expect(onStub.callCount).to.equal(1);
        expect(workerOnStub.callCount).to.equal(1);
        expect(workerConnectionStub.callCount).to.equal(1);
        expect(removeAllListenersWorkerStub.callCount).to.equal(1);
        expect(pushworker.connected).to.equal(true);
        done();
      })
      .catch((err) => done(err));
  });

  it("does mark a task as successful", (done) => {
    let workerData = {doStuff: true};
    let {
      pushworker,
      onStub,
      workerOnStub,
      pushConnectionStub,
      workerConnectionStub,
      ackStub,
      socketStub
    } = generateTestResources("task", "dead", {workerData: workerData});

    pushworker.on("task", (task) => {
      // PUSH and WORKER sockets should be connected
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      expect(workerOnStub.callCount).to.equal(1);
      expect(pushworker.connected).to.equal(true);
      expect(task.task).to.eql(workerData);

      // mark the task as successfully completed
      task.success();

      expect(ackStub.callCount).to.equal(1);
      done();
    });

    pushworker.connect()
      .then(() => pushworker.subscribe())
      .catch((error) => done(error));
  });

  it("does mark a task as failed", (done) => {
    let workerData = {doStuff: false};
    let {
      pushworker,
      onStub,
      workerOnStub,
      pushWriteStub,
      pushConnectionStub,
      workerConnectionStub,
      ackStub,
      socketStub
    } = generateTestResources("task", "dead", {workerData: workerData});

    pushworker.on("task", (task) => {
      // PUSH and WORKER sockets should be connected
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      expect(workerOnStub.callCount).to.equal(1);
      expect(pushworker.connected).to.equal(true);
      expect(task.task).to.eql(workerData);

      // mark the task as successfully completed
      task.fail();

      expect(ackStub.callCount).to.equal(1);

      // make sure we wrote a new message
      setTimeout(() => {
        let workerMessage = [JSON.stringify({
          task: workerData,
          retries: 9,
          retryWait: 0
        }), "utf8"];

        expect(pushWriteStub.callCount).to.equal(1);
        expect(workerMessage).to.eql(pushWriteStub.lastCall.args);
        done();
      });
    });

    pushworker.connect()
      .then(() => pushworker.subscribe())
      .catch((error) => done(error));
  });

  it("does run a failing task until it's dumped into a DL queue", (done) => {
    let workerData = {doStuff: false};
    let {
      pushworker,
      onStub,
      workerOnStub,
      pushWriteStub,
      pushConnectionStub,
      workerConnectionStub,
      ackStub,
      socketStub
    } = generateTestResources("task", "dead", {
      workerData: workerData,
      lastRetry: true
    });

    pushworker.on("task", (task) => {
      // PUSH and WORKER sockets should be connected
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      expect(workerOnStub.callCount).to.equal(1);
      expect(pushworker.connected).to.equal(true);
      expect(task.task).to.eql(workerData);

      // mark the task as successfully completed
      task.fail();

      expect(ackStub.callCount).to.equal(1);

      // make sure the task was enqueued to the dead letter queue
      let workerMessage = [JSON.stringify({
        queue: "task",
        task: workerData
      }), "utf8"];
      expect(pushWriteStub.callCount).to.equal(1);
      expect(workerMessage).to.eql(pushWriteStub.lastCall.args);

      done();

    });

    pushworker.connect()
      .then(() => pushworker.subscribe())
      .catch((error) => done(error));
  });

  it("does disconnect from push and worker streams", (done) => {
    let {
      pushworker,
      onStub,
      pushConnectionStub,
      workerConnectionStub,
      socketStub,
      closeStub
    } = generateTestResources("task", "dead");


    // connect -> disconnect
    pushworker.connect().then(() => pushworker.disconnect()).then(() => {
      // PUSH and WORKER sockets should have been connected once each
      expect(socketStub.callCount).to.equal(3);
      expect(pushConnectionStub.callCount).to.equal(2);
      expect(workerConnectionStub.callCount).to.equal(1);
      expect(onStub.callCount).to.equal(1);
      // PUSH and WORKER sockets should have been disconnected once each
      expect(closeStub.callCount).to.equal(3);
      expect(pushworker.connected).to.equal(false);
      done();
    }).catch((error) => done(error));
  });
});
