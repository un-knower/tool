include "TCLIService.thrift"
namespace java com.hiido.hcat.thrift.protocol

enum recode {
	SUCESS = 1,
	FAILURE = 2
}

enum JobStatus {
	READY,
	RUNNING,
	COMPLETE,
	FAILURE,
	CANCEL
}

struct CommitQuery {
	1: required map<string,string> cipher,
	2: required string query,
	3: optional map<string,string> conf
}

struct CommitQueryReply {
	1: required recode code,
	2: required string queryId,
	3: required bool quick,
	4: required JobStatus status, 
	5: optional string stderr
}

struct CancelQuery {
	1: required map<string,string> cipher,
	2: required string queryId
}

struct CancelQueryReply {
	1: required recode code
}

struct QueryStatus {
	1: required map<string,string> cipher,
	2: required string queryId
}

struct QueryStatusReply {
	1: required recode code,
	2: required list<string> jobs,
	3: required list<TCLIService.TTableSchema> schemata,
	4: required list<string> resultDirs,
	5: required JobStatus status,
	6: optional i64 startTime,
	7: optional i64 endTime,
	8: optional string stderr,
	9: optional double progress
}

struct LoadFile {
	1: required map<string,string> cipher,
	2: required string query,
	3: required string currentDatabase,
	4: optional map<string, string> conf
}

struct LoadFileReply {
	1: required recode code,
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