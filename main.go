package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	redis "github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	taskQueueName = "taskqueue"
	workingQueue  = "workingQueue"
)

type TaskData struct {
	Id   string
	Data string
}

func main() {
	var redisAddress string
	flag.StringVar(&redisAddress, "redisaddr", "127.0.0.1:6379", "The address and port of the redis instance")
	var isProducer bool
	flag.BoolVar(&isProducer, "isproducer", false, "If true, this will submit test jobs for processing by workers")
	var taskCount int
	flag.IntVar(&taskCount, "tasks", 10, "number of tasks the producer should make")
	flag.Parse()
	client := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	if isProducer {
		produceTasks(client, taskCount)
	} else {
		w := NewWorker()
		w.RunWorker(client)
	}

	// _, err := client.Exists("hashKey").Result()
	// if err != nil {
	// 	panic(err)
	// }

}

func produceTasks(r *redis.Client, taskcount int) {
	// this will inject the 10 tasks, and wait for them all to be processed
	for i := 0; i < taskcount; i++ {
		td := TaskData{
			Id:   uuid.New().String(),
			Data: fmt.Sprintf("%v", i),
		}
		td_ser, err := json.Marshal(td)
		if err != nil {
			panic(err)
		}
		r.LPush(taskQueueName, td_ser)
	}
	fmt.Println(fmt.Sprintf("sent %v jobs", taskcount))
	for {
		time.Sleep(time.Second)
		jobsleft := r.LLen(taskQueueName).Val()
		currentExec := r.LLen(workingQueue).Val()
		fmt.Println(fmt.Sprintf("Remaining: %v, Active: %v", jobsleft, currentExec))
		if currentExec == 0 && jobsleft == 0 {
			break
		}
	}

}

type WorkerState string

const (
	None   = WorkerState("none")
	Slave  = WorkerState("slave")
	Master = WorkerState("master")
)

type worker struct {
	ID          string
	CurrentTask *TaskData
	CurrState   WorkerState
}

func NewWorker() *worker {
	return &worker{
		ID:        uuid.New().String(),
		CurrState: None}
}

const masterkey = "masterkey"

func masterDuties(ctx context.Context, r *redis.Client) error {
	for {
		select {
		case <- ctx.Done():
			return nil
			case 
		}
	}
}

func (w *worker) RunWorker(r *redis.Client) {
	//workerUUID := uuid.New()
	eg, ctx := errgroup.WithContext(context.Background())

	eg.Go(func() error {

		/*
			This routine is the master monitor. When a worker starts, it tries to see if someone is the master. If they are, then it subscribes to updates.
			If after a set amount of time
		*/
		var sub *redis.PubSub
		defer func() {
			if sub != nil {
				sub.Close()
			}
		}()

		for {
			switch w.CurrState {
			case None:
				// we are at an initial setup.
				if res, err := r.SetNX(masterkey, w.ID, time.Second*10).Result(); err == nil {
					if !res {
						// we didn't set it. we are a slave
						w.CurrState = Slave
						fmt.Println("Im a slave")
						if sub == nil {
							sub = r.Subscribe("workerchannel")
						}
					} else {
						// we set it. We are the mastah
						w.CurrState = Master
						fmt.Println("Im the master")
						if sub != nil {
							sub.Close()
							sub = nil
						}
						eg.Go(func() error {
							// Master ping worker. Lets other other slaves know whos boss (literally) at a regular interval
							for {
								select {
								case <-time.After(time.Second * 5):
									r.Expire(masterkey, time.Second*10)
									r.Publish("workerchannel", "keepalive")

								case <-ctx.Done():
									return nil
								}
							}
						})
					}
				} else {
					return err
				}
			case Master:
				// a master does its master responsibilities.

			case Slave:
				// a slave doesn't do much, other than wait for the opportunity to become master itself. Slaves simply expect to recieve a message after a certain time. If not,
				// they revert to None and try to become the master

				select {
				case msg := <-sub.Channel():
					_ = msg // dont care for now
				case <-time.After(time.Second * 10):
					// we didn't hear anything in 10 seconds. My master is DEAD. Time to become the master maybe?
					w.CurrState = None
					fmt.Println("Master is gone. Attempting a promotion")
				}

			}
		}
		// Keepalive

	})
	eg.Go(func() error {
		// worker

		for {
			res, err := r.BRPopLPush(taskQueueName, workingQueue, time.Second*10).Result()
			if err == redis.Nil {
				continue
			} else if err != nil {
				return err
			}
			var data TaskData
			json.Unmarshal([]byte(res), &data)
			taskOwnershipFinishChan := make(chan struct{}, 1)   // push to this to release ownership of task
			taskOwnershipReleasedChan := make(chan struct{}, 1) // read from this to be notified when the task is finished
			eg.Go(func() error {
				return setTaskOwnership(r, w.ID, data.Id, taskOwnershipFinishChan, taskOwnershipReleasedChan)
			})
			executeTask(ctx, &data)
			taskOwnershipFinishChan <- struct{}{} // request that the task ownership be released
			// while this happends, lets cleanup
			if remCount, err := r.LRem(workingQueue, -1, res).Result(); err != nil {
				return errors.Wrapf(err, "failed to remove taskid %v from work queue", data.Id)
			} else if remCount != 1 {
				return fmt.Errorf("unexpected remove count: %v", remCount)
			}
			<-taskOwnershipReleasedChan // now wait until task ownership is released before moving on

		}
	})
	eg.Wait()
}

func getTaskOwnerKey(taskId string) string {
	return fmt.Sprintf("taskworker:%v", taskId)
}

// will block until cancelled
func setTaskOwnership(r *redis.Client, myId string, taskId string, cancelChan <-chan struct{}, doneChan chan<- struct{}) error {
	defer func() {
		doneChan <- struct{}{} // push something to the done chan so someone can know that the worker key lock is now available
	}()
	workerKey := getTaskOwnerKey(taskId)
	r.Set(workerKey, myId, time.Second*10)
MainLoop:
	for {
		select {
		case <-cancelChan:
			break MainLoop
		case <-time.After(time.Second * 5):
			if err := r.Expire(workerKey, time.Second*10).Err(); err != nil { // extend the timeout by another 10 seconds
				return errors.Wrapf(err, "failed to extend key expiry on %v", workerKey)
			}
		}
	}
	// once done, release the key
	if err := r.Del(workerKey).Err(); err != nil {
		return errors.Wrapf(err, "failed to delete key %v", workerKey)
	}

	return nil
}

func executeTask(ctx context.Context, taskdata *TaskData) {
	time.Sleep(time.Second * 1)
	fmt.Println(fmt.Sprintf("Processed task: %v", taskdata.Data))
}
