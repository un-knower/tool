include "hcat.thrift"
namespace java com.hiido.hcat.thrift.protocol
namespace php Thrift.Php.HcatClient

struct SignupReply {
	1: required i32 code = 0,
	2: required string retMessage
}

service SignupService{
    SignupReply signup(1: i32 companyId, 2: string companyName, 3: i32 userId) throws (1:hcat.AuthorizationException authException, 2: hcat.RuntimeException runTimeException)
}