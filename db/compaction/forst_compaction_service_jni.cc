#include "forst_compaction_service_jni.h"

#include <iostream>
#include <vector>

#include "forst_compaction_service.h"

using namespace ROCKSDB_NAMESPACE;
using namespace ROCKSDB_NAMESPACE::Service::Compaction;
JNIEXPORT jobject JNICALL
Java_org_apache_flink_state_forst_service_compaction_CompactionServiceJNI_handleCompactionRequest(
    JNIEnv* env, jclass, jbyteArray compaction_param_bytes,
    jbyteArray serialized_file_mappings) {
  void* params =
      env->GetPrimitiveArrayCritical(compaction_param_bytes, nullptr);
  jsize param_bytes_size = env->GetArrayLength(compaction_param_bytes);
  std::vector<char> param_bytes(param_bytes_size);
  memcpy(param_bytes.data(), params, param_bytes_size);
  env->ReleasePrimitiveArrayCritical(compaction_param_bytes, params, 0);

  auto [info, input, base_path] =
      SerializationUtils::deserializeCompactionParams(param_bytes);
  //std::cout << "Received compaction request: base path: " << base_path
//            << std::endl;
  jobject fs_jobject = ForStCompactionService::createFileSystem(base_path);
  ForStCompactionService::registerFileMappings(fs_jobject,
                                               serialized_file_mappings);
  std::string output = ForStCompactionService::triggerCompaction(
      info, input, base_path, fs_jobject);
  ForStCompactionService::registerCompactionOutput(fs_jobject, output);

  //std::cout << "handleCompactionRequest complete: " << base_path << std::endl;
  return fs_jobject;
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forst_service_compaction_PrimaryDBClientJNI_handleCompactionResponse(
    JNIEnv* env, jclass, jbyteArray output_jbyte_array,
    jlong ongoing_compaction_handle) {
  void* output_ptr =
      env->GetPrimitiveArrayCritical(output_jbyte_array, nullptr);
  jsize output_bytes_size = env->GetArrayLength(output_jbyte_array);
  auto* ongoing_compaction =
      reinterpret_cast<OngoingCompaction*>(ongoing_compaction_handle);
  ongoing_compaction->out =
      std::string(static_cast<const char*>(output_ptr), output_bytes_size);
  ongoing_compaction->returned = true;
  //std::cout << "handleCompactionResponse complete: " << ongoing_compaction->out
//            << std::endl;
  env->ReleasePrimitiveArrayCritical(output_jbyte_array, output_ptr, 0);
}
