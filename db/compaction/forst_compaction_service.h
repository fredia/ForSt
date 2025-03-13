#pragma once

#include <iostream>
#include <utility>

#include "env/flink/jni_helper.h"
#include "env/flink/jvm_util.h"
#include "logging/logging.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
namespace Service::Compaction {
class SerializationUtils {
 public:
  struct CompactionParams {
    CompactionServiceJobInfo info;
    std::string input;
    std::string base_path;
  };

  static std::vector<char> serializeCompactionParams(
      const CompactionServiceJobInfo& info, const std::string& input,
      const std::string& base_path);

  static std::string serializeCompactionInputFiles(
      const std::vector<std::string>& input_files);

  static std::vector<char> serializeCompactionOutput(const std::string& output);

  static CompactionParams deserializeCompactionParams(
      const std::vector<char>& bytes);

  // For debug, compute hash code of compaction params
  static uint32_t hashCode(const CompactionServiceJobInfo& info,
                           const std::string& input);

 private:
  template <class T>
  static inline void serialize(char*& cur_data, const T& value) {
    if constexpr (std::is_same<T, std::string>::value) {
      auto str_size = static_cast<uint32_t>(value.size());
      *reinterpret_cast<uint32_t*>(cur_data) = str_size;
      cur_data += sizeof(uint32_t);
      memcpy(cur_data, value.data(), str_size);
      cur_data += str_size;
    } else {
      *reinterpret_cast<T*>(cur_data) = value;
      cur_data += sizeof(T);
    }
  }

  template <class T>
  static inline T deserialize(const char*& cur_data) {
    if constexpr (std::is_same<T, std::string>::value) {
      uint32_t str_size = *reinterpret_cast<const uint32_t*>(cur_data);
      cur_data += sizeof(uint32_t);
      std::string str;
      std::copy(cur_data, cur_data + str_size, back_inserter(str));
      cur_data += str_size;
      return str;
    } else {
      T value = *reinterpret_cast<const T*>(cur_data);
      cur_data += sizeof(T);
      return value;
    }
  }
};

class DBShadow {
 public:
  DB* db_;

  DBShadow() : db_(nullptr) {}
};

class OngoingCompaction {
 public:
  OngoingCompaction() : returned(false) {}

  std::string out;

  bool returned;
};
}  // namespace Service::Compaction

class ForStCompactionService : public CompactionService {
 public:
  explicit ForStCompactionService(std::shared_ptr<Logger> logger)
      : logger_(std::move(logger)), jni_env_(getJNIEnv()) {
    ROCKS_LOG_INFO(logger_, "jni_env_: %p", jni_env_);
  }

  void setLogger(std::shared_ptr<Logger> logger) {
    logger_ = std::move(logger);
    ROCKS_LOG_INFO(logger_, "set_logger: %p", jni_env_);
  }

  void setHostDB(DB* db) { host_db_ = db; }

  ~ForStCompactionService() override = default;

  const char* Name() const override { return "ForStCompactionService"; }

  // [Client-side] Request a remote compaction.
  CompactionServiceJobStatus StartV2(const CompactionServiceJobInfo& info,
                                     const std::string& input,
                                     const std::vector<std::string>&) override;

  // Wait for remote compaction to finish.
  CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& /*info*/,
      std::string* /*compaction_service_result*/) override;

  static jobject createFileSystem(const std::string& base_path);

  static void registerFileMappings(const jobject& fs_jobject,
                                   const jarray& serialized_file_mappings);

  static void registerCompactionOutput(const jobject& fs_jobject,
                                       const std::string& output);

  // [Server-side] Process a remote compaction request.
  static std::string triggerCompaction(const CompactionServiceJobInfo& info,
                                       const std::string& input,
                                       const std::string& base_path,
                                       jobject fs_jobject);

 public:
  static inline Service::Compaction::DBShadow db_shadow_{};

 private:
  DB* host_db_;
  std::shared_ptr<Logger> logger_;
  JNIEnv* jni_env_;
  std::unordered_map<uint64_t, Service::Compaction::OngoingCompaction*>
      ongoing_compactions_;
  static constexpr std::string_view kClientJNIHelperClassName =
      "org/apache/flink/state/forst/service/compaction/PrimaryDBClientJNI";
  static constexpr std::string_view kClientJNIHelperMethodName =
      "invokeCompactionService";
  static constexpr std::string_view kClientJNIHelperMethodSignature =
      "([BLjava/lang/String;Ljava/lang/Object;J)V";
  static constexpr std::string_view kForStFlinkFileSystemClassName =
      "org/apache/flink/state/forst/fs/StringifiedForStFileSystem";
  static constexpr std::string_view kForStFlinkFileSystemGetMethodName = "getForCompactionService";
  static constexpr std::string_view kForStFlinkFileSystemGetMethodSignature =
      "(Ljava/lang/String;)Lorg/apache/flink/state/forst/fs/"
      "StringifiedForStFileSystem;";
  static constexpr std::string_view kCompactionServiceJNIClassName =
      "org/apache/flink/state/forst/service/compaction/CompactionServiceJNI";
  static constexpr std::string_view
      kCompactionServiceJNIRegisterFileMappingsMethodName =
          "registerFileMappings";
  static constexpr std::string_view
      kCompactionServiceJNIRegisterFileMappingsMethodSignature =
          "(Ljava/lang/Object;[B)V";
  static constexpr std::string_view
      kCompactionServiceJNIRegisterOutputMethodName = "registerOutput";
  static constexpr std::string_view
      kCompactionServiceJNIRegisterOutputMethodSignature =
          "(Ljava/lang/Object;[B)V";

  Status callJavaCompactionService(
      const std::vector<char>& bytes, const std::vector<std::string>& input_files,
      const jobject& fs_jobject,
      Service::Compaction::OngoingCompaction& ongoing_compaction);
};
}  // namespace ROCKSDB_NAMESPACE