#pragma once
#include <jni.h>
/* Header for class org_apache_flink_state_forst_service_compaction_CompactionServiceJNI */

#ifndef _Included_org_apache_flink_state_forst_service_compaction_CompactionServiceJNI
#define _Included_org_apache_flink_state_forst_service_compaction_CompactionServiceJNI
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_flink_state_forst_service_compaction_CompactionServiceJNI
 * Method:    handleCompactionRequest
 * Signature: ([B[B)[B
 */
JNIEXPORT jobject JNICALL
Java_org_apache_flink_state_forst_service_compaction_CompactionServiceJNI_handleCompactionRequest(
    JNIEnv* env, jclass, jbyteArray compaction_param_bytes,
    jbyteArray serialized_file_mappings) ;

/*
 * Class:     org_apache_flink_state_forst_service_compaction_PrimaryDBClientJNI
 * Method:    handleCompactionResponse
 * Signature: (Ljava/lang/String;J)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_state_forst_service_compaction_PrimaryDBClientJNI_handleCompactionResponse
    (JNIEnv *, jclass, jbyteArray, jlong);

#ifdef __cplusplus
}
#endif
#endif
