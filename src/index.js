import { EventEmitter } from "events";
import { createContext } from "rabbit.js";

import { CREATE_CONTEXT, MQ_CONTEXT } from "./symbols";

export class PushWorker extends EventEmitter {
  constructor(queue, deadLetterQueue, url, options={}) {
    super();
    this[MQ_CONTEXT] = this[CREATE_CONTEXT](url);
    this.queue = queue;
    this.deadLetterQueue = deadLetterQueue;
    this.connected = false;
    this.pushSocket = null;
    this.workerSocket = null;
    this.concurrency = options.concurrency || 1; // number of concurrent workers
  }

  [CREATE_CONTEXT] (url) {
    return createContext(url);
  }

  connect () {
    return new Promise((resolve) => {
      if (this.connected) {
        resolve();
      } else {
        this[MQ_CONTEXT].on("ready", () => {
          this.pushSocket = this[MQ_CONTEXT].socket("PUSH");
          this.pushDeadLetterSocket = this[MQ_CONTEXT].socket("PUSH");

          // prefetch determines the number of unacknowledged messages
          // that can be received at a time, persistant means that
          // rabbitMQ will keep unsent messages on disk so they
          // won't be lost on restart
          this.workerSocket = this[MQ_CONTEXT].socket("WORKER", {
            prefetch: this.concurrency, persistant: true
          });

          let workerPromise = new Promise((workerResolve) => {
            this.workerSocket.connect(this.queue, () => workerResolve());
          });
          let pushPromise = new Promise((pushResolve) => {
            this.pushSocket.connect(
              this.queue,
              () => pushResolve()
            );
          });
          let pushDeadLetterPromise = new Promise((pushDeadLetterResolve) => {
            this.pushDeadLetterSocket.connect(
              this.deadLetterQueue,
              () => pushDeadLetterResolve()
            );
          });


          // wait for both publish and subscribe connections
          Promise.all([workerPromise, pushPromise, pushDeadLetterPromise])
          .then(() => {
            this.connected = true;
            resolve();
          });
        });
      }
    });
  }

  disconnect () {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject();
      } else {
        this.pushSocket.close();
        this.workerSocket.close();
        this.pushDeadLetterSocket.close();
        this.connected = false;
        resolve();
      }
    });
  }

  push (task, options={}) {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject();
      } else {
        let message = {
          task: task,
          retries: options.retries === undefined ? 10 : options.retries,
          // seconds to wait to retry
          retryWait: options.retryWait === undefined ? 10 : options.retryWait
        };
        this.pushSocket.write(JSON.stringify(message), "utf8");
        resolve();
      }
    });
  }

  subscribe () {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject();
      } else {
        this.workerSocket.on("data", (data) => {
          let message = JSON.parse(data);
          this.emit("task", {
            task: message.task,
            success: () => this.workerSocket.ack(),
            fail: () => {
              // remove the message from the queue
              this.workerSocket.ack();

              let retriesLeft = message.retries - 1;
              if (retriesLeft > 0) {
                // have retries left to process
                // place it back on the queue after waiting
                // the right amount of time
                setTimeout(() => {
                  this.push(message.task, {
                    retries: retriesLeft,
                    retryWait: message.retryWait
                  });
                }, message.retryWait * 1000);
              } else {
                // no more retries, dump it into the dead letter queue
                this.pushDeadLetterSocket.write(
                  JSON.stringify({
                    queue: this.queue,
                    task: message.task
                  }),
                  "utf8"
                );
              }
            }
          });
        });
        resolve();
      }
    });
  }

  unsubscribe () {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject();
      } else {
        this.workerSocket.removeAllListeners("data");
        resolve();
      }
    });
  }
}
