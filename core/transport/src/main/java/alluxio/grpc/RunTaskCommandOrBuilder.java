// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/job_master.proto

package alluxio.grpc;

public interface RunTaskCommandOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.job.RunTaskCommand)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 jobId = 1;</code>
   * @return Whether the jobId field is set.
   */
  boolean hasJobId();
  /**
   * <code>optional int64 jobId = 1;</code>
   * @return The jobId.
   */
  long getJobId();

  /**
   * <code>optional int64 taskId = 2;</code>
   * @return Whether the taskId field is set.
   */
  boolean hasTaskId();
  /**
   * <code>optional int64 taskId = 2;</code>
   * @return The taskId.
   */
  long getTaskId();

  /**
   * <code>optional bytes jobConfig = 3;</code>
   * @return Whether the jobConfig field is set.
   */
  boolean hasJobConfig();
  /**
   * <code>optional bytes jobConfig = 3;</code>
   * @return The jobConfig.
   */
  com.google.protobuf.ByteString getJobConfig();

  /**
   * <code>optional bytes taskArgs = 4;</code>
   * @return Whether the taskArgs field is set.
   */
  boolean hasTaskArgs();
  /**
   * <code>optional bytes taskArgs = 4;</code>
   * @return The taskArgs.
   */
  com.google.protobuf.ByteString getTaskArgs();
}