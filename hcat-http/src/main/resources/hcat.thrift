namespace java com.hiido.hcat.thrift.protocol
namespace php Thrift.Php.HcatClient

enum recode {
	SUCESS = 0,
	FAILURE = 1
}

enum JobStatus {
	READY=0,
	RUNNING=1,
	COMPLETE=2,
	FAILURE=3,
	DEAD=4,
	HBTIMEOUT=5,
	CANCEL=6
}

struct CommitQuery {
	1: required map<string,string> cipher,
	2: required string query,
	3: optional map<string,string> conf
}

struct Handle {
	1: required string queryId,
	2: required bool quick,
	3: optional string stdout,
	4: optional string stderr,
	5: required i32 totalN,
	6: required bool running = false
}

struct CommitQueryReply {
	1: required i32 reCode = 0,
	2: optional string retMessage, 
	3: required Handle handle
}

struct CancelQuery {
	1: required map<string,string> cipher,
	2: required string queryId
}

struct CancelQueryReply {
	1: required i32 reCode = 0,
	2: optional string retMessage
}

struct QueryStatus {
	1: required map<string,string> cipher,
	2: required string queryId
}

struct Field {
	1: required string name,
	2: required string type
}

struct QueryProgress {
	1: required list<string> jobId,
	2: required i32 state,
	3: required i32 n,
	4: required string engine,
	5: required string errmsg,
	6: optional string res,
	7: optional list<Field> fields,
	8: required i64 startTime = 0,
	9: required i64 endTime = 0,
	10: required i64 resSize = 0,
	11: required double progress,
	12: required bool isFetchTask,
	13: optional list<string> fetchDirs
}

struct QueryStatusReply {
	1: required i32 retCode = 0,
	2: optional string retMessage,
	3: required QueryProgress queryProgress
}

struct LoadFile {
	1: required map<string,string> cipher,
	2: required string query,
	3: required string currentDatabase,
	4: optional map<string, string> conf
}

struct LoadFileReply {
	1: required i32 code = 0,
	2: required string tmpPath
}

exception AuthorizationException{
	1: string msg
}

exception NotFoundException {
	1: string msg
}

exception RuntimeException {
	1: string msg
}


service CliService{
	CommitQueryReply commit(1: CommitQuery cq) throws (1:AuthorizationException authException, 2: RuntimeException runTimeException),
	QueryStatusReply queryJobStatus(1: QueryStatus qs) throws(1: NotFoundException notFoundException),
	CancelQueryReply cancelJob(1: CancelQuery cq) throws (1:AuthorizationException authException, 2: NotFoundException notFoundException),
	LoadFileReply laodData(1: LoadFile lf) throws (1:AuthorizationException authException, 2: RuntimeException runTimeException)
}