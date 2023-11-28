API_PROTO_FILES=$(shell find groupcachepb -name "*.proto")
API_PROTO_FILES_PATH=./groupcachepb
TEST_PROTO_FILES=$(shell find testpb -name "*.proto")
TEST_PROTO_FILES_PATH=./testpb


.PHONY: api
# generate api proto
api:
	protoc --proto_path=${API_PROTO_FILES_PATH} \
	--go_out=paths=source_relative:${API_PROTO_FILES_PATH} \
	$(API_PROTO_FILES)

.PHONY: testpb
# generate test proto
testpb:
	protoc --proto_path=${TEST_PROTO_FILES_PATH} \
	--go_out=paths=source_relative:${TEST_PROTO_FILES_PATH} \
	$(TEST_PROTO_FILES)

.PHONY: test
# run test
test:
	go test -v ./...