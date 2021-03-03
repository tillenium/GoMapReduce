package distributed


/**
Workers to query if the Map/Reduce tasks are available
 */

type CheckForMapTasksCompletionRequest struct {

}

type CheckForMapTasksCompletionResponse struct {
	AllCompleted bool
}

type CheckForReduceTasksCompletionRequest struct {

}

type CheckForReducdTasksCompletionResponse struct {
	AllCompleted bool
}


/**
Maps Related APIs
 */

type GetMapTaskRequest struct{

}

type GetMapTaskResponse struct {
	Filename string
	TaskId int //negative if no tasks available
	NumReduce int
}

type UpdateMapTaskRequest struct {
	TaskId int
}

type UpdateMapTaskResponse struct {

}


/**
Reduce Related APIs
*/

type GetReduceTaskRequest struct{

}

type GetReduceTaskResponse struct {
	Filename string
	TaskId int //negative if no tasks available
}

type UpdateReduceTaskRequest struct {
	TaskId int
}

type UpdateReduceTaskResponse struct {

}