#include "forst_compaction_service.h"

#include "db/db_impl/db_impl.h"
#include "env/flink/env_flink.h"

namespace ROCKSDB_NAMESPACE {
namespace Service::Compaction {
std::vector<char> SerializationUtils::serializeCompactionParams(
    const forstdb::CompactionServiceJobInfo& info, const std::string& input,
    const std::string& base_path) {
  // calculate size (use uint32_t to store each string size)
  size_t size = 0;
  // -- info --
  size += 3 * sizeof(uint32_t) + info.db_name.size() + info.db_id.size() +
          info.db_session_id.size() + sizeof(info.job_id) +
          sizeof(info.priority);
  // -- input --
  size += sizeof(uint32_t) + input.size();
  // -- base_path --
  size += sizeof(uint32_t) + base_path.size();

  // serialize
  std::vector<char> bytes(size);
  char* cur_data = bytes.data();
  for (const auto& str : {info.db_name, info.db_id, info.db_session_id}) {
    serialize<std::string>(cur_data, str);
  }
  serialize<uint64_t>(cur_data, info.job_id);
  serialize<char>(cur_data, info.priority);
  serialize<std::string>(cur_data, input);
  serialize<std::string>(cur_data, base_path);

  return bytes;
}

std::string SerializationUtils::serializeCompactionInputFiles(
    const std::vector<std::string>& input_files) {
  if (input_files.empty()) {
    return "";
  }

  std::ostringstream oss;
  auto it = input_files.begin();

  oss << *it++;
  while (it != input_files.end()) {
    oss << ":" << *it++;
  }

  return oss.str();
}

std::vector<char> SerializationUtils::serializeCompactionOutput(
    const std::string& output) {
  std::vector<char> bytes(output.size());
  char* cur_data = bytes.data();
  memcpy(cur_data, output.data(), output.size());
  return bytes;
}

SerializationUtils::CompactionParams
SerializationUtils::deserializeCompactionParams(
    const std::vector<char>& bytes) {
  const char* cur_data = bytes.data();
  auto db_name = deserialize<std::string>(cur_data);
  //std::cout << "deserializeCompactionParams db_name: " << db_name << std::endl;
  auto db_id = deserialize<std::string>(cur_data);
  auto db_session_id = deserialize<std::string>(cur_data);
  auto job_id = deserialize<uint64_t>(cur_data);
  auto priority = static_cast<Env::Priority>(deserialize<char>(cur_data));
  CompactionServiceJobInfo info(db_name, db_id, db_session_id, job_id,
                                priority);
  auto input = deserialize<std::string>(cur_data);
  //std::cout << "deserializeCompactionParams input: " << input << std::endl;
  auto base_path = deserialize<std::string>(cur_data);
  //std::cout << "deserializeCompactionParams base_path: " << base_path
//            << std::endl;
  return {info, input, base_path};
}

uint32_t SerializationUtils::hashCode(const CompactionServiceJobInfo& info,
                                      const std::string& input) {
  // compute hash code by xor every 4 bytes  from info and input
  uint32_t hash = 0;
  for (const auto& str :
       {info.db_name, info.db_id, info.db_session_id, input}) {
    for (size_t i = 0; i < str.size(); i += 4) {
      hash ^= *reinterpret_cast<const uint32_t*>(str.data() + i);
    }
  }
  hash ^= info.job_id;
  hash ^= static_cast<uint32_t>(info.priority);
  return hash;
}
}  // namespace Service::Compaction

CompactionServiceJobStatus ForStCompactionService::StartV2(
    const CompactionServiceJobInfo& info, const std::string& input, const std::vector<std::string>& input_files) {
  ROCKS_LOG_INFO(
      logger_, "StartV2 for remote compaction, hashCode of params: %u",
      Service::Compaction::SerializationUtils::hashCode(info, input));

  const auto* fs =
      reinterpret_cast<FlinkFileSystem*>(host_db_->GetFileSystem());
  const auto& bash_path = fs->getBasePath();
  const auto& fs_jobject = fs->getFileSystemInstance();
  //std::cout << "bash_path: " << bash_path << std::endl;
  const std::vector<char> serialized_param_bytes =
      Service::Compaction::SerializationUtils::serializeCompactionParams(
          info, input, bash_path);

  std::cout << "StartV2 input: " << input << std::endl;
  auto* ongoing_compaction = new Service::Compaction::OngoingCompaction();
  ongoing_compactions_.emplace(info.job_id, ongoing_compaction);

//  callJavaCompactionService(serialized_param_bytes, input_files, fs_jobject,
//                            *ongoing_compaction);
  return CompactionServiceJobStatus::kUseLocal;
}

CompactionServiceJobStatus ForStCompactionService::WaitForCompleteV2(
    const CompactionServiceJobInfo& info, std::string* output) {
  auto* ongoing_compaction = ongoing_compactions_[info.job_id];
  ongoing_compactions_.erase(info.job_id);
  //std::cout << "WaitForCompleteV2: " << info.job_id << std::endl;
  while (!ongoing_compaction->returned) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  *output = ongoing_compaction->out;
  delete ongoing_compaction;

  //std::cout << "get output: " << *output << std::endl;
  return CompactionServiceJobStatus::kSuccess;
}

jobject ForStCompactionService::createFileSystem(const std::string& base_path) {
  auto* jni_env = getJNIEnv();
  thread_local jclass fs_class =
      jni_env->FindClass(kForStFlinkFileSystemClassName.data());
  thread_local jmethodID get_method = jni_env->GetStaticMethodID(
      fs_class, kForStFlinkFileSystemGetMethodName.data(),
      kForStFlinkFileSystemGetMethodSignature.data());
  std::cout << "createFileSystem: " << kForStFlinkFileSystemGetMethodName.data() << std::endl;
  jstring uriStringArg = jni_env->NewStringUTF(base_path.c_str());
  jobject fileSystemInstance =
      jni_env->CallStaticObjectMethod(fs_class, get_method, uriStringArg);
  jni_env->DeleteLocalRef(uriStringArg);
  return fileSystemInstance;
}

void ForStCompactionService::registerFileMappings(
    const jobject& fs_jobject, const jarray& serialized_file_mappings) {
  auto* jni_env = getJNIEnv();
  thread_local jclass jni_class =
      jni_env->FindClass(kCompactionServiceJNIClassName.data());
  thread_local jmethodID invoke_compaction_service_method =
      jni_env->GetStaticMethodID(
          jni_class, kCompactionServiceJNIRegisterFileMappingsMethodName.data(),
          kCompactionServiceJNIRegisterFileMappingsMethodSignature.data());
  jni_env->CallStaticObjectMethod(jni_class, invoke_compaction_service_method,
                                  fs_jobject, serialized_file_mappings);
}

void ForStCompactionService::registerCompactionOutput(
    const jobject& fs_jobject, const std::string& output) {
  //std::cout << "registerCompactionOutput: " << std::endl;
  auto* jni_env = getJNIEnv();
  thread_local jclass jni_class =
      jni_env->FindClass(kCompactionServiceJNIClassName.data());
  thread_local jmethodID method = jni_env->GetStaticMethodID(
      jni_class, kCompactionServiceJNIRegisterOutputMethodName.data(),
      kCompactionServiceJNIRegisterOutputMethodSignature.data());
  if (jni_class == nullptr || method == nullptr) {
    //std::cout << "registerCompactionOutput: null" << std::endl;
  }

  auto output_bytes = Service::Compaction::SerializationUtils::serializeCompactionOutput(output);
  jbyteArray output_jbyte_array = jni_env->NewByteArray(output_bytes.size());
  jni_env->SetByteArrayRegion(output_jbyte_array, 0, output_bytes.size(), (jbyte*)output_bytes.data());
  jni_env->CallStaticObjectMethod(jni_class, method, fs_jobject, output_jbyte_array);
  jni_env->DeleteLocalRef(output_jbyte_array);
}

std::string ForStCompactionService::triggerCompaction(
    const CompactionServiceJobInfo& info, const std::string& input,
    const std::string& base_path, jobject fs_jobject) {
  std::unique_ptr<FileSystem> fs_unique_ptr = nullptr;
  FlinkFileSystem::Create(FileSystem::Default(), base_path, &fs_unique_ptr,
                          fs_jobject);
  std::shared_ptr<FileSystem> fs_shared_ptr = std::move(fs_unique_ptr);

  std::unique_ptr<Env> env = NewCompositeEnv(fs_shared_ptr);
  const auto* shadow_db =
      reinterpret_cast<DBImpl*>(ForStCompactionService::db_shadow_.db_);
  Options shadow_options = shadow_db->GetOptions();
  CompactionServiceOptionsOverride options_override;
  options_override.env = env.get();
  options_override.file_checksum_gen_factory =
      shadow_options.file_checksum_gen_factory;
  options_override.comparator = shadow_options.comparator;
  options_override.merge_operator = shadow_options.merge_operator;
  options_override.compaction_filter = shadow_options.compaction_filter;
  options_override.compaction_filter_factory =
      shadow_options.compaction_filter_factory;
  options_override.prefix_extractor = shadow_options.prefix_extractor;
  options_override.table_factory = shadow_options.table_factory;
  options_override.sst_partitioner_factory =
      shadow_options.sst_partitioner_factory;
  options_override.statistics = nullptr;

  OpenAndCompactOptions options;
  for (size_t i = 0; i < shadow_options.db_paths.size(); ++i) {
    //std::cout << "db_paths: " << shadow_options.db_paths[i].path
//              << ", number: " << i << std::endl;
  }
  //std::cout << "input: " << input << std::endl;
  //std::cout << "base_path: " << base_path << std::endl;

  std::string output;
  Status s = DB::OpenAndCompact(options, "db", shadow_options.db_paths[0].path,
                                input, &output, options_override);
  //std::cout << "output: " << output << std::endl;
  return output;
}

Status ForStCompactionService::callJavaCompactionService(
    const std::vector<char>& bytes, const std::vector<std::string>& input_files, const jobject& fs_jobject,
    Service::Compaction::OngoingCompaction& ongoing_compaction) {
  // get Java class and method
  jni_env_ = getJNIEnv();
  thread_local jclass client_class =
      jni_env_->FindClass(kClientJNIHelperClassName.data());
  if (client_class == nullptr) {
    ROCKS_LOG_INFO(logger_, "fail to get client class");
    return Status::InvalidArgument("Failed to find client class");
  }
  thread_local jmethodID invoke_compaction_service_method =
      jni_env_->GetStaticMethodID(client_class,
                                  kClientJNIHelperMethodName.data(),
                                  kClientJNIHelperMethodSignature.data());
  //std::cout << "invoke_compaction_service_method: "
//            << kClientJNIHelperMethodName.data() << ", "
//            << kClientJNIHelperMethodSignature.data() << std::endl;
  if (invoke_compaction_service_method == nullptr) {
    ROCKS_LOG_INFO(logger_, "fail to get client class or method");
    return Status::InvalidArgument("Failed to find client class or method");
  }

  // call java method
  jbyteArray array = jni_env_->NewByteArray(bytes.size());
  jni_env_->SetByteArrayRegion(array, 0, bytes.size(), (jbyte*)bytes.data());
  std::string input_files_str = Service::Compaction::SerializationUtils::serializeCompactionInputFiles(input_files);
  jstring input_file_string_arg = jni_env_->NewStringUTF(input_files_str.c_str());

  jni_env_->CallStaticObjectMethod(
      client_class, invoke_compaction_service_method, array, input_file_string_arg, fs_jobject,
      reinterpret_cast<jlong>(&ongoing_compaction));
  jni_env_->DeleteLocalRef(array);
  jni_env_->DeleteLocalRef(input_file_string_arg);

  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
