# Taskpool POC

VSCode tasks have been added to start and stop redis. Once built, the Taskpool exe will give you the required prompts.

Example usage:
* Start one or more instaces of taskpool without parameters. It will run in service mode. They will figure out amongst themselves who is the master
* Try killing the master. You will notice that a slave will get promoted after about 10 seconds
* try executing `taskpool -isproducer -tasks 100`. 100 jobs will be submitted to the queue and the workers will "work" through them to completion

Logic for abandonned task recovery has not been implemented into the master cycle yet.