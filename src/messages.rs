//! Messages used by the Kafka protocol.
//!
//! These messages are generated programmatically. See the [Kafka's protocol documentation](https://kafka.apache.org/protocol.html) for more information about a given message type.
// WARNING: the items of this module are generated and should not be edited directly.

use crate::error::Result;
#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
use crate::protocol::Decodable;
#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
use crate::protocol::Encodable;
#[cfg(all(feature = "client", feature = "broker"))]
use crate::protocol::Request;
use crate::protocol::VersionRange;
use crate::protocol::{HeaderVersion, NewType, StrBytes};
use std::convert::TryFrom;

pub mod consumer_protocol_assignment;
pub use consumer_protocol_assignment::ConsumerProtocolAssignment;

pub mod consumer_protocol_subscription;
pub use consumer_protocol_subscription::ConsumerProtocolSubscription;

pub mod default_principal_data;
pub use default_principal_data::DefaultPrincipalData;

pub mod end_txn_marker;
pub use end_txn_marker::EndTxnMarker;

pub mod k_raft_version_record;
pub use k_raft_version_record::KRaftVersionRecord;

pub mod leader_change_message;
pub use leader_change_message::LeaderChangeMessage;

pub mod request_header;
pub use request_header::RequestHeader;

pub mod response_header;
pub use response_header::ResponseHeader;

pub mod snapshot_footer_record;
pub use snapshot_footer_record::SnapshotFooterRecord;

pub mod snapshot_header_record;
pub use snapshot_header_record::SnapshotHeaderRecord;

pub mod voters_record;
pub use voters_record::VotersRecord;

pub mod produce_request;
pub use produce_request::ProduceRequest;

pub mod fetch_request;
pub use fetch_request::FetchRequest;

pub mod list_offsets_request;
pub use list_offsets_request::ListOffsetsRequest;

pub mod metadata_request;
pub use metadata_request::MetadataRequest;

pub mod offset_commit_request;
pub use offset_commit_request::OffsetCommitRequest;

pub mod offset_fetch_request;
pub use offset_fetch_request::OffsetFetchRequest;

pub mod find_coordinator_request;
pub use find_coordinator_request::FindCoordinatorRequest;

pub mod join_group_request;
pub use join_group_request::JoinGroupRequest;

pub mod heartbeat_request;
pub use heartbeat_request::HeartbeatRequest;

pub mod leave_group_request;
pub use leave_group_request::LeaveGroupRequest;

pub mod sync_group_request;
pub use sync_group_request::SyncGroupRequest;

pub mod describe_groups_request;
pub use describe_groups_request::DescribeGroupsRequest;

pub mod list_groups_request;
pub use list_groups_request::ListGroupsRequest;

pub mod sasl_handshake_request;
pub use sasl_handshake_request::SaslHandshakeRequest;

pub mod api_versions_request;
pub use api_versions_request::ApiVersionsRequest;

pub mod create_topics_request;
pub use create_topics_request::CreateTopicsRequest;

pub mod delete_topics_request;
pub use delete_topics_request::DeleteTopicsRequest;

pub mod delete_records_request;
pub use delete_records_request::DeleteRecordsRequest;

pub mod init_producer_id_request;
pub use init_producer_id_request::InitProducerIdRequest;

pub mod offset_for_leader_epoch_request;
pub use offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;

pub mod add_partitions_to_txn_request;
pub use add_partitions_to_txn_request::AddPartitionsToTxnRequest;

pub mod add_offsets_to_txn_request;
pub use add_offsets_to_txn_request::AddOffsetsToTxnRequest;

pub mod end_txn_request;
pub use end_txn_request::EndTxnRequest;

pub mod write_txn_markers_request;
pub use write_txn_markers_request::WriteTxnMarkersRequest;

pub mod txn_offset_commit_request;
pub use txn_offset_commit_request::TxnOffsetCommitRequest;

pub mod describe_acls_request;
pub use describe_acls_request::DescribeAclsRequest;

pub mod create_acls_request;
pub use create_acls_request::CreateAclsRequest;

pub mod delete_acls_request;
pub use delete_acls_request::DeleteAclsRequest;

pub mod describe_configs_request;
pub use describe_configs_request::DescribeConfigsRequest;

pub mod alter_configs_request;
pub use alter_configs_request::AlterConfigsRequest;

pub mod alter_replica_log_dirs_request;
pub use alter_replica_log_dirs_request::AlterReplicaLogDirsRequest;

pub mod describe_log_dirs_request;
pub use describe_log_dirs_request::DescribeLogDirsRequest;

pub mod sasl_authenticate_request;
pub use sasl_authenticate_request::SaslAuthenticateRequest;

pub mod create_partitions_request;
pub use create_partitions_request::CreatePartitionsRequest;

pub mod create_delegation_token_request;
pub use create_delegation_token_request::CreateDelegationTokenRequest;

pub mod renew_delegation_token_request;
pub use renew_delegation_token_request::RenewDelegationTokenRequest;

pub mod expire_delegation_token_request;
pub use expire_delegation_token_request::ExpireDelegationTokenRequest;

pub mod describe_delegation_token_request;
pub use describe_delegation_token_request::DescribeDelegationTokenRequest;

pub mod delete_groups_request;
pub use delete_groups_request::DeleteGroupsRequest;

pub mod elect_leaders_request;
pub use elect_leaders_request::ElectLeadersRequest;

pub mod incremental_alter_configs_request;
pub use incremental_alter_configs_request::IncrementalAlterConfigsRequest;

pub mod alter_partition_reassignments_request;
pub use alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;

pub mod list_partition_reassignments_request;
pub use list_partition_reassignments_request::ListPartitionReassignmentsRequest;

pub mod offset_delete_request;
pub use offset_delete_request::OffsetDeleteRequest;

pub mod describe_client_quotas_request;
pub use describe_client_quotas_request::DescribeClientQuotasRequest;

pub mod alter_client_quotas_request;
pub use alter_client_quotas_request::AlterClientQuotasRequest;

pub mod describe_user_scram_credentials_request;
pub use describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest;

pub mod alter_user_scram_credentials_request;
pub use alter_user_scram_credentials_request::AlterUserScramCredentialsRequest;

pub mod vote_request;
pub use vote_request::VoteRequest;

pub mod begin_quorum_epoch_request;
pub use begin_quorum_epoch_request::BeginQuorumEpochRequest;

pub mod end_quorum_epoch_request;
pub use end_quorum_epoch_request::EndQuorumEpochRequest;

pub mod describe_quorum_request;
pub use describe_quorum_request::DescribeQuorumRequest;

pub mod alter_partition_request;
pub use alter_partition_request::AlterPartitionRequest;

pub mod update_features_request;
pub use update_features_request::UpdateFeaturesRequest;

pub mod envelope_request;
pub use envelope_request::EnvelopeRequest;

pub mod fetch_snapshot_request;
pub use fetch_snapshot_request::FetchSnapshotRequest;

pub mod describe_cluster_request;
pub use describe_cluster_request::DescribeClusterRequest;

pub mod describe_producers_request;
pub use describe_producers_request::DescribeProducersRequest;

pub mod broker_registration_request;
pub use broker_registration_request::BrokerRegistrationRequest;

pub mod broker_heartbeat_request;
pub use broker_heartbeat_request::BrokerHeartbeatRequest;

pub mod unregister_broker_request;
pub use unregister_broker_request::UnregisterBrokerRequest;

pub mod describe_transactions_request;
pub use describe_transactions_request::DescribeTransactionsRequest;

pub mod list_transactions_request;
pub use list_transactions_request::ListTransactionsRequest;

pub mod allocate_producer_ids_request;
pub use allocate_producer_ids_request::AllocateProducerIdsRequest;

pub mod consumer_group_heartbeat_request;
pub use consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

pub mod consumer_group_describe_request;
pub use consumer_group_describe_request::ConsumerGroupDescribeRequest;

pub mod controller_registration_request;
pub use controller_registration_request::ControllerRegistrationRequest;

pub mod get_telemetry_subscriptions_request;
pub use get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest;

pub mod push_telemetry_request;
pub use push_telemetry_request::PushTelemetryRequest;

pub mod assign_replicas_to_dirs_request;
pub use assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;

pub mod list_config_resources_request;
pub use list_config_resources_request::ListConfigResourcesRequest;

pub mod describe_topic_partitions_request;
pub use describe_topic_partitions_request::DescribeTopicPartitionsRequest;

pub mod share_group_heartbeat_request;
pub use share_group_heartbeat_request::ShareGroupHeartbeatRequest;

pub mod share_group_describe_request;
pub use share_group_describe_request::ShareGroupDescribeRequest;

pub mod share_fetch_request;
pub use share_fetch_request::ShareFetchRequest;

pub mod share_acknowledge_request;
pub use share_acknowledge_request::ShareAcknowledgeRequest;

pub mod add_raft_voter_request;
pub use add_raft_voter_request::AddRaftVoterRequest;

pub mod remove_raft_voter_request;
pub use remove_raft_voter_request::RemoveRaftVoterRequest;

pub mod update_raft_voter_request;
pub use update_raft_voter_request::UpdateRaftVoterRequest;

pub mod initialize_share_group_state_request;
pub use initialize_share_group_state_request::InitializeShareGroupStateRequest;

pub mod read_share_group_state_request;
pub use read_share_group_state_request::ReadShareGroupStateRequest;

pub mod write_share_group_state_request;
pub use write_share_group_state_request::WriteShareGroupStateRequest;

pub mod delete_share_group_state_request;
pub use delete_share_group_state_request::DeleteShareGroupStateRequest;

pub mod read_share_group_state_summary_request;
pub use read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest;

pub mod describe_share_group_offsets_request;
pub use describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest;

pub mod alter_share_group_offsets_request;
pub use alter_share_group_offsets_request::AlterShareGroupOffsetsRequest;

pub mod delete_share_group_offsets_request;
pub use delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest;

pub mod produce_response;
pub use produce_response::ProduceResponse;

pub mod fetch_response;
pub use fetch_response::FetchResponse;

pub mod list_offsets_response;
pub use list_offsets_response::ListOffsetsResponse;

pub mod metadata_response;
pub use metadata_response::MetadataResponse;

pub mod offset_commit_response;
pub use offset_commit_response::OffsetCommitResponse;

pub mod offset_fetch_response;
pub use offset_fetch_response::OffsetFetchResponse;

pub mod find_coordinator_response;
pub use find_coordinator_response::FindCoordinatorResponse;

pub mod join_group_response;
pub use join_group_response::JoinGroupResponse;

pub mod heartbeat_response;
pub use heartbeat_response::HeartbeatResponse;

pub mod leave_group_response;
pub use leave_group_response::LeaveGroupResponse;

pub mod sync_group_response;
pub use sync_group_response::SyncGroupResponse;

pub mod describe_groups_response;
pub use describe_groups_response::DescribeGroupsResponse;

pub mod list_groups_response;
pub use list_groups_response::ListGroupsResponse;

pub mod sasl_handshake_response;
pub use sasl_handshake_response::SaslHandshakeResponse;

pub mod api_versions_response;
pub use api_versions_response::ApiVersionsResponse;

pub mod create_topics_response;
pub use create_topics_response::CreateTopicsResponse;

pub mod delete_topics_response;
pub use delete_topics_response::DeleteTopicsResponse;

pub mod delete_records_response;
pub use delete_records_response::DeleteRecordsResponse;

pub mod init_producer_id_response;
pub use init_producer_id_response::InitProducerIdResponse;

pub mod offset_for_leader_epoch_response;
pub use offset_for_leader_epoch_response::OffsetForLeaderEpochResponse;

pub mod add_partitions_to_txn_response;
pub use add_partitions_to_txn_response::AddPartitionsToTxnResponse;

pub mod add_offsets_to_txn_response;
pub use add_offsets_to_txn_response::AddOffsetsToTxnResponse;

pub mod end_txn_response;
pub use end_txn_response::EndTxnResponse;

pub mod write_txn_markers_response;
pub use write_txn_markers_response::WriteTxnMarkersResponse;

pub mod txn_offset_commit_response;
pub use txn_offset_commit_response::TxnOffsetCommitResponse;

pub mod describe_acls_response;
pub use describe_acls_response::DescribeAclsResponse;

pub mod create_acls_response;
pub use create_acls_response::CreateAclsResponse;

pub mod delete_acls_response;
pub use delete_acls_response::DeleteAclsResponse;

pub mod describe_configs_response;
pub use describe_configs_response::DescribeConfigsResponse;

pub mod alter_configs_response;
pub use alter_configs_response::AlterConfigsResponse;

pub mod alter_replica_log_dirs_response;
pub use alter_replica_log_dirs_response::AlterReplicaLogDirsResponse;

pub mod describe_log_dirs_response;
pub use describe_log_dirs_response::DescribeLogDirsResponse;

pub mod sasl_authenticate_response;
pub use sasl_authenticate_response::SaslAuthenticateResponse;

pub mod create_partitions_response;
pub use create_partitions_response::CreatePartitionsResponse;

pub mod create_delegation_token_response;
pub use create_delegation_token_response::CreateDelegationTokenResponse;

pub mod renew_delegation_token_response;
pub use renew_delegation_token_response::RenewDelegationTokenResponse;

pub mod expire_delegation_token_response;
pub use expire_delegation_token_response::ExpireDelegationTokenResponse;

pub mod describe_delegation_token_response;
pub use describe_delegation_token_response::DescribeDelegationTokenResponse;

pub mod delete_groups_response;
pub use delete_groups_response::DeleteGroupsResponse;

pub mod elect_leaders_response;
pub use elect_leaders_response::ElectLeadersResponse;

pub mod incremental_alter_configs_response;
pub use incremental_alter_configs_response::IncrementalAlterConfigsResponse;

pub mod alter_partition_reassignments_response;
pub use alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;

pub mod list_partition_reassignments_response;
pub use list_partition_reassignments_response::ListPartitionReassignmentsResponse;

pub mod offset_delete_response;
pub use offset_delete_response::OffsetDeleteResponse;

pub mod describe_client_quotas_response;
pub use describe_client_quotas_response::DescribeClientQuotasResponse;

pub mod alter_client_quotas_response;
pub use alter_client_quotas_response::AlterClientQuotasResponse;

pub mod describe_user_scram_credentials_response;
pub use describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;

pub mod alter_user_scram_credentials_response;
pub use alter_user_scram_credentials_response::AlterUserScramCredentialsResponse;

pub mod vote_response;
pub use vote_response::VoteResponse;

pub mod begin_quorum_epoch_response;
pub use begin_quorum_epoch_response::BeginQuorumEpochResponse;

pub mod end_quorum_epoch_response;
pub use end_quorum_epoch_response::EndQuorumEpochResponse;

pub mod describe_quorum_response;
pub use describe_quorum_response::DescribeQuorumResponse;

pub mod alter_partition_response;
pub use alter_partition_response::AlterPartitionResponse;

pub mod update_features_response;
pub use update_features_response::UpdateFeaturesResponse;

pub mod envelope_response;
pub use envelope_response::EnvelopeResponse;

pub mod fetch_snapshot_response;
pub use fetch_snapshot_response::FetchSnapshotResponse;

pub mod describe_cluster_response;
pub use describe_cluster_response::DescribeClusterResponse;

pub mod describe_producers_response;
pub use describe_producers_response::DescribeProducersResponse;

pub mod broker_registration_response;
pub use broker_registration_response::BrokerRegistrationResponse;

pub mod broker_heartbeat_response;
pub use broker_heartbeat_response::BrokerHeartbeatResponse;

pub mod unregister_broker_response;
pub use unregister_broker_response::UnregisterBrokerResponse;

pub mod describe_transactions_response;
pub use describe_transactions_response::DescribeTransactionsResponse;

pub mod list_transactions_response;
pub use list_transactions_response::ListTransactionsResponse;

pub mod allocate_producer_ids_response;
pub use allocate_producer_ids_response::AllocateProducerIdsResponse;

pub mod consumer_group_heartbeat_response;
pub use consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;

pub mod consumer_group_describe_response;
pub use consumer_group_describe_response::ConsumerGroupDescribeResponse;

pub mod controller_registration_response;
pub use controller_registration_response::ControllerRegistrationResponse;

pub mod get_telemetry_subscriptions_response;
pub use get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse;

pub mod push_telemetry_response;
pub use push_telemetry_response::PushTelemetryResponse;

pub mod assign_replicas_to_dirs_response;
pub use assign_replicas_to_dirs_response::AssignReplicasToDirsResponse;

pub mod list_config_resources_response;
pub use list_config_resources_response::ListConfigResourcesResponse;

pub mod describe_topic_partitions_response;
pub use describe_topic_partitions_response::DescribeTopicPartitionsResponse;

pub mod share_group_heartbeat_response;
pub use share_group_heartbeat_response::ShareGroupHeartbeatResponse;

pub mod share_group_describe_response;
pub use share_group_describe_response::ShareGroupDescribeResponse;

pub mod share_fetch_response;
pub use share_fetch_response::ShareFetchResponse;

pub mod share_acknowledge_response;
pub use share_acknowledge_response::ShareAcknowledgeResponse;

pub mod add_raft_voter_response;
pub use add_raft_voter_response::AddRaftVoterResponse;

pub mod remove_raft_voter_response;
pub use remove_raft_voter_response::RemoveRaftVoterResponse;

pub mod update_raft_voter_response;
pub use update_raft_voter_response::UpdateRaftVoterResponse;

pub mod initialize_share_group_state_response;
pub use initialize_share_group_state_response::InitializeShareGroupStateResponse;

pub mod read_share_group_state_response;
pub use read_share_group_state_response::ReadShareGroupStateResponse;

pub mod write_share_group_state_response;
pub use write_share_group_state_response::WriteShareGroupStateResponse;

pub mod delete_share_group_state_response;
pub use delete_share_group_state_response::DeleteShareGroupStateResponse;

pub mod read_share_group_state_summary_response;
pub use read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse;

pub mod describe_share_group_offsets_response;
pub use describe_share_group_offsets_response::DescribeShareGroupOffsetsResponse;

pub mod alter_share_group_offsets_response;
pub use alter_share_group_offsets_response::AlterShareGroupOffsetsResponse;

pub mod delete_share_group_offsets_response;
pub use delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse;

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ProduceRequest {
    const KEY: i16 = 0;
    type Response = ProduceResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for FetchRequest {
    const KEY: i16 = 1;
    type Response = FetchResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ListOffsetsRequest {
    const KEY: i16 = 2;
    type Response = ListOffsetsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for MetadataRequest {
    const KEY: i16 = 3;
    type Response = MetadataResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for OffsetCommitRequest {
    const KEY: i16 = 8;
    type Response = OffsetCommitResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for OffsetFetchRequest {
    const KEY: i16 = 9;
    type Response = OffsetFetchResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for FindCoordinatorRequest {
    const KEY: i16 = 10;
    type Response = FindCoordinatorResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for JoinGroupRequest {
    const KEY: i16 = 11;
    type Response = JoinGroupResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for HeartbeatRequest {
    const KEY: i16 = 12;
    type Response = HeartbeatResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for LeaveGroupRequest {
    const KEY: i16 = 13;
    type Response = LeaveGroupResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for SyncGroupRequest {
    const KEY: i16 = 14;
    type Response = SyncGroupResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeGroupsRequest {
    const KEY: i16 = 15;
    type Response = DescribeGroupsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ListGroupsRequest {
    const KEY: i16 = 16;
    type Response = ListGroupsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for SaslHandshakeRequest {
    const KEY: i16 = 17;
    type Response = SaslHandshakeResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ApiVersionsRequest {
    const KEY: i16 = 18;
    type Response = ApiVersionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for CreateTopicsRequest {
    const KEY: i16 = 19;
    type Response = CreateTopicsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteTopicsRequest {
    const KEY: i16 = 20;
    type Response = DeleteTopicsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteRecordsRequest {
    const KEY: i16 = 21;
    type Response = DeleteRecordsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for InitProducerIdRequest {
    const KEY: i16 = 22;
    type Response = InitProducerIdResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for OffsetForLeaderEpochRequest {
    const KEY: i16 = 23;
    type Response = OffsetForLeaderEpochResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AddPartitionsToTxnRequest {
    const KEY: i16 = 24;
    type Response = AddPartitionsToTxnResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AddOffsetsToTxnRequest {
    const KEY: i16 = 25;
    type Response = AddOffsetsToTxnResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for EndTxnRequest {
    const KEY: i16 = 26;
    type Response = EndTxnResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for WriteTxnMarkersRequest {
    const KEY: i16 = 27;
    type Response = WriteTxnMarkersResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for TxnOffsetCommitRequest {
    const KEY: i16 = 28;
    type Response = TxnOffsetCommitResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeAclsRequest {
    const KEY: i16 = 29;
    type Response = DescribeAclsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for CreateAclsRequest {
    const KEY: i16 = 30;
    type Response = CreateAclsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteAclsRequest {
    const KEY: i16 = 31;
    type Response = DeleteAclsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeConfigsRequest {
    const KEY: i16 = 32;
    type Response = DescribeConfigsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterConfigsRequest {
    const KEY: i16 = 33;
    type Response = AlterConfigsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterReplicaLogDirsRequest {
    const KEY: i16 = 34;
    type Response = AlterReplicaLogDirsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeLogDirsRequest {
    const KEY: i16 = 35;
    type Response = DescribeLogDirsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for SaslAuthenticateRequest {
    const KEY: i16 = 36;
    type Response = SaslAuthenticateResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for CreatePartitionsRequest {
    const KEY: i16 = 37;
    type Response = CreatePartitionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for CreateDelegationTokenRequest {
    const KEY: i16 = 38;
    type Response = CreateDelegationTokenResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for RenewDelegationTokenRequest {
    const KEY: i16 = 39;
    type Response = RenewDelegationTokenResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ExpireDelegationTokenRequest {
    const KEY: i16 = 40;
    type Response = ExpireDelegationTokenResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeDelegationTokenRequest {
    const KEY: i16 = 41;
    type Response = DescribeDelegationTokenResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteGroupsRequest {
    const KEY: i16 = 42;
    type Response = DeleteGroupsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ElectLeadersRequest {
    const KEY: i16 = 43;
    type Response = ElectLeadersResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for IncrementalAlterConfigsRequest {
    const KEY: i16 = 44;
    type Response = IncrementalAlterConfigsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterPartitionReassignmentsRequest {
    const KEY: i16 = 45;
    type Response = AlterPartitionReassignmentsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ListPartitionReassignmentsRequest {
    const KEY: i16 = 46;
    type Response = ListPartitionReassignmentsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for OffsetDeleteRequest {
    const KEY: i16 = 47;
    type Response = OffsetDeleteResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeClientQuotasRequest {
    const KEY: i16 = 48;
    type Response = DescribeClientQuotasResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterClientQuotasRequest {
    const KEY: i16 = 49;
    type Response = AlterClientQuotasResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeUserScramCredentialsRequest {
    const KEY: i16 = 50;
    type Response = DescribeUserScramCredentialsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterUserScramCredentialsRequest {
    const KEY: i16 = 51;
    type Response = AlterUserScramCredentialsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for VoteRequest {
    const KEY: i16 = 52;
    type Response = VoteResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for BeginQuorumEpochRequest {
    const KEY: i16 = 53;
    type Response = BeginQuorumEpochResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for EndQuorumEpochRequest {
    const KEY: i16 = 54;
    type Response = EndQuorumEpochResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeQuorumRequest {
    const KEY: i16 = 55;
    type Response = DescribeQuorumResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterPartitionRequest {
    const KEY: i16 = 56;
    type Response = AlterPartitionResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for UpdateFeaturesRequest {
    const KEY: i16 = 57;
    type Response = UpdateFeaturesResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for EnvelopeRequest {
    const KEY: i16 = 58;
    type Response = EnvelopeResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for FetchSnapshotRequest {
    const KEY: i16 = 59;
    type Response = FetchSnapshotResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeClusterRequest {
    const KEY: i16 = 60;
    type Response = DescribeClusterResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeProducersRequest {
    const KEY: i16 = 61;
    type Response = DescribeProducersResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for BrokerRegistrationRequest {
    const KEY: i16 = 62;
    type Response = BrokerRegistrationResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for BrokerHeartbeatRequest {
    const KEY: i16 = 63;
    type Response = BrokerHeartbeatResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for UnregisterBrokerRequest {
    const KEY: i16 = 64;
    type Response = UnregisterBrokerResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeTransactionsRequest {
    const KEY: i16 = 65;
    type Response = DescribeTransactionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ListTransactionsRequest {
    const KEY: i16 = 66;
    type Response = ListTransactionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AllocateProducerIdsRequest {
    const KEY: i16 = 67;
    type Response = AllocateProducerIdsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ConsumerGroupHeartbeatRequest {
    const KEY: i16 = 68;
    type Response = ConsumerGroupHeartbeatResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ConsumerGroupDescribeRequest {
    const KEY: i16 = 69;
    type Response = ConsumerGroupDescribeResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ControllerRegistrationRequest {
    const KEY: i16 = 70;
    type Response = ControllerRegistrationResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for GetTelemetrySubscriptionsRequest {
    const KEY: i16 = 71;
    type Response = GetTelemetrySubscriptionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for PushTelemetryRequest {
    const KEY: i16 = 72;
    type Response = PushTelemetryResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AssignReplicasToDirsRequest {
    const KEY: i16 = 73;
    type Response = AssignReplicasToDirsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ListConfigResourcesRequest {
    const KEY: i16 = 74;
    type Response = ListConfigResourcesResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeTopicPartitionsRequest {
    const KEY: i16 = 75;
    type Response = DescribeTopicPartitionsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ShareGroupHeartbeatRequest {
    const KEY: i16 = 76;
    type Response = ShareGroupHeartbeatResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ShareGroupDescribeRequest {
    const KEY: i16 = 77;
    type Response = ShareGroupDescribeResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ShareFetchRequest {
    const KEY: i16 = 78;
    type Response = ShareFetchResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ShareAcknowledgeRequest {
    const KEY: i16 = 79;
    type Response = ShareAcknowledgeResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AddRaftVoterRequest {
    const KEY: i16 = 80;
    type Response = AddRaftVoterResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for RemoveRaftVoterRequest {
    const KEY: i16 = 81;
    type Response = RemoveRaftVoterResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for UpdateRaftVoterRequest {
    const KEY: i16 = 82;
    type Response = UpdateRaftVoterResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for InitializeShareGroupStateRequest {
    const KEY: i16 = 83;
    type Response = InitializeShareGroupStateResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ReadShareGroupStateRequest {
    const KEY: i16 = 84;
    type Response = ReadShareGroupStateResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for WriteShareGroupStateRequest {
    const KEY: i16 = 85;
    type Response = WriteShareGroupStateResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteShareGroupStateRequest {
    const KEY: i16 = 86;
    type Response = DeleteShareGroupStateResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for ReadShareGroupStateSummaryRequest {
    const KEY: i16 = 87;
    type Response = ReadShareGroupStateSummaryResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DescribeShareGroupOffsetsRequest {
    const KEY: i16 = 90;
    type Response = DescribeShareGroupOffsetsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for AlterShareGroupOffsetsRequest {
    const KEY: i16 = 91;
    type Response = AlterShareGroupOffsetsResponse;
}

#[cfg(all(feature = "client", feature = "broker"))]
impl Request for DeleteShareGroupOffsetsRequest {
    const KEY: i16 = 92;
    type Response = DeleteShareGroupOffsetsResponse;
}

/// Valid API keys in the Kafka protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKey {
    /// API key for request ProduceRequest
    Produce = 0,
    /// API key for request FetchRequest
    Fetch = 1,
    /// API key for request ListOffsetsRequest
    ListOffsets = 2,
    /// API key for request MetadataRequest
    Metadata = 3,
    /// API key for request OffsetCommitRequest
    OffsetCommit = 8,
    /// API key for request OffsetFetchRequest
    OffsetFetch = 9,
    /// API key for request FindCoordinatorRequest
    FindCoordinator = 10,
    /// API key for request JoinGroupRequest
    JoinGroup = 11,
    /// API key for request HeartbeatRequest
    Heartbeat = 12,
    /// API key for request LeaveGroupRequest
    LeaveGroup = 13,
    /// API key for request SyncGroupRequest
    SyncGroup = 14,
    /// API key for request DescribeGroupsRequest
    DescribeGroups = 15,
    /// API key for request ListGroupsRequest
    ListGroups = 16,
    /// API key for request SaslHandshakeRequest
    SaslHandshake = 17,
    /// API key for request ApiVersionsRequest
    ApiVersions = 18,
    /// API key for request CreateTopicsRequest
    CreateTopics = 19,
    /// API key for request DeleteTopicsRequest
    DeleteTopics = 20,
    /// API key for request DeleteRecordsRequest
    DeleteRecords = 21,
    /// API key for request InitProducerIdRequest
    InitProducerId = 22,
    /// API key for request OffsetForLeaderEpochRequest
    OffsetForLeaderEpoch = 23,
    /// API key for request AddPartitionsToTxnRequest
    AddPartitionsToTxn = 24,
    /// API key for request AddOffsetsToTxnRequest
    AddOffsetsToTxn = 25,
    /// API key for request EndTxnRequest
    EndTxn = 26,
    /// API key for request WriteTxnMarkersRequest
    WriteTxnMarkers = 27,
    /// API key for request TxnOffsetCommitRequest
    TxnOffsetCommit = 28,
    /// API key for request DescribeAclsRequest
    DescribeAcls = 29,
    /// API key for request CreateAclsRequest
    CreateAcls = 30,
    /// API key for request DeleteAclsRequest
    DeleteAcls = 31,
    /// API key for request DescribeConfigsRequest
    DescribeConfigs = 32,
    /// API key for request AlterConfigsRequest
    AlterConfigs = 33,
    /// API key for request AlterReplicaLogDirsRequest
    AlterReplicaLogDirs = 34,
    /// API key for request DescribeLogDirsRequest
    DescribeLogDirs = 35,
    /// API key for request SaslAuthenticateRequest
    SaslAuthenticate = 36,
    /// API key for request CreatePartitionsRequest
    CreatePartitions = 37,
    /// API key for request CreateDelegationTokenRequest
    CreateDelegationToken = 38,
    /// API key for request RenewDelegationTokenRequest
    RenewDelegationToken = 39,
    /// API key for request ExpireDelegationTokenRequest
    ExpireDelegationToken = 40,
    /// API key for request DescribeDelegationTokenRequest
    DescribeDelegationToken = 41,
    /// API key for request DeleteGroupsRequest
    DeleteGroups = 42,
    /// API key for request ElectLeadersRequest
    ElectLeaders = 43,
    /// API key for request IncrementalAlterConfigsRequest
    IncrementalAlterConfigs = 44,
    /// API key for request AlterPartitionReassignmentsRequest
    AlterPartitionReassignments = 45,
    /// API key for request ListPartitionReassignmentsRequest
    ListPartitionReassignments = 46,
    /// API key for request OffsetDeleteRequest
    OffsetDelete = 47,
    /// API key for request DescribeClientQuotasRequest
    DescribeClientQuotas = 48,
    /// API key for request AlterClientQuotasRequest
    AlterClientQuotas = 49,
    /// API key for request DescribeUserScramCredentialsRequest
    DescribeUserScramCredentials = 50,
    /// API key for request AlterUserScramCredentialsRequest
    AlterUserScramCredentials = 51,
    /// API key for request VoteRequest
    Vote = 52,
    /// API key for request BeginQuorumEpochRequest
    BeginQuorumEpoch = 53,
    /// API key for request EndQuorumEpochRequest
    EndQuorumEpoch = 54,
    /// API key for request DescribeQuorumRequest
    DescribeQuorum = 55,
    /// API key for request AlterPartitionRequest
    AlterPartition = 56,
    /// API key for request UpdateFeaturesRequest
    UpdateFeatures = 57,
    /// API key for request EnvelopeRequest
    Envelope = 58,
    /// API key for request FetchSnapshotRequest
    FetchSnapshot = 59,
    /// API key for request DescribeClusterRequest
    DescribeCluster = 60,
    /// API key for request DescribeProducersRequest
    DescribeProducers = 61,
    /// API key for request BrokerRegistrationRequest
    BrokerRegistration = 62,
    /// API key for request BrokerHeartbeatRequest
    BrokerHeartbeat = 63,
    /// API key for request UnregisterBrokerRequest
    UnregisterBroker = 64,
    /// API key for request DescribeTransactionsRequest
    DescribeTransactions = 65,
    /// API key for request ListTransactionsRequest
    ListTransactions = 66,
    /// API key for request AllocateProducerIdsRequest
    AllocateProducerIds = 67,
    /// API key for request ConsumerGroupHeartbeatRequest
    ConsumerGroupHeartbeat = 68,
    /// API key for request ConsumerGroupDescribeRequest
    ConsumerGroupDescribe = 69,
    /// API key for request ControllerRegistrationRequest
    ControllerRegistration = 70,
    /// API key for request GetTelemetrySubscriptionsRequest
    GetTelemetrySubscriptions = 71,
    /// API key for request PushTelemetryRequest
    PushTelemetry = 72,
    /// API key for request AssignReplicasToDirsRequest
    AssignReplicasToDirs = 73,
    /// API key for request ListConfigResourcesRequest
    ListConfigResources = 74,
    /// API key for request DescribeTopicPartitionsRequest
    DescribeTopicPartitions = 75,
    /// API key for request ShareGroupHeartbeatRequest
    ShareGroupHeartbeat = 76,
    /// API key for request ShareGroupDescribeRequest
    ShareGroupDescribe = 77,
    /// API key for request ShareFetchRequest
    ShareFetch = 78,
    /// API key for request ShareAcknowledgeRequest
    ShareAcknowledge = 79,
    /// API key for request AddRaftVoterRequest
    AddRaftVoter = 80,
    /// API key for request RemoveRaftVoterRequest
    RemoveRaftVoter = 81,
    /// API key for request UpdateRaftVoterRequest
    UpdateRaftVoter = 82,
    /// API key for request InitializeShareGroupStateRequest
    InitializeShareGroupState = 83,
    /// API key for request ReadShareGroupStateRequest
    ReadShareGroupState = 84,
    /// API key for request WriteShareGroupStateRequest
    WriteShareGroupState = 85,
    /// API key for request DeleteShareGroupStateRequest
    DeleteShareGroupState = 86,
    /// API key for request ReadShareGroupStateSummaryRequest
    ReadShareGroupStateSummary = 87,
    /// API key for request DescribeShareGroupOffsetsRequest
    DescribeShareGroupOffsets = 90,
    /// API key for request AlterShareGroupOffsetsRequest
    AlterShareGroupOffsets = 91,
    /// API key for request DeleteShareGroupOffsetsRequest
    DeleteShareGroupOffsets = 92,
}

impl ApiKey {
    /// Get the version of request header that needs to be prepended to this message
    pub fn request_header_version(&self, version: i16) -> i16 {
        match self {
            ApiKey::Produce => ProduceRequest::header_version(version),
            ApiKey::Fetch => FetchRequest::header_version(version),
            ApiKey::ListOffsets => ListOffsetsRequest::header_version(version),
            ApiKey::Metadata => MetadataRequest::header_version(version),
            ApiKey::OffsetCommit => OffsetCommitRequest::header_version(version),
            ApiKey::OffsetFetch => OffsetFetchRequest::header_version(version),
            ApiKey::FindCoordinator => FindCoordinatorRequest::header_version(version),
            ApiKey::JoinGroup => JoinGroupRequest::header_version(version),
            ApiKey::Heartbeat => HeartbeatRequest::header_version(version),
            ApiKey::LeaveGroup => LeaveGroupRequest::header_version(version),
            ApiKey::SyncGroup => SyncGroupRequest::header_version(version),
            ApiKey::DescribeGroups => DescribeGroupsRequest::header_version(version),
            ApiKey::ListGroups => ListGroupsRequest::header_version(version),
            ApiKey::SaslHandshake => SaslHandshakeRequest::header_version(version),
            ApiKey::ApiVersions => ApiVersionsRequest::header_version(version),
            ApiKey::CreateTopics => CreateTopicsRequest::header_version(version),
            ApiKey::DeleteTopics => DeleteTopicsRequest::header_version(version),
            ApiKey::DeleteRecords => DeleteRecordsRequest::header_version(version),
            ApiKey::InitProducerId => InitProducerIdRequest::header_version(version),
            ApiKey::OffsetForLeaderEpoch => OffsetForLeaderEpochRequest::header_version(version),
            ApiKey::AddPartitionsToTxn => AddPartitionsToTxnRequest::header_version(version),
            ApiKey::AddOffsetsToTxn => AddOffsetsToTxnRequest::header_version(version),
            ApiKey::EndTxn => EndTxnRequest::header_version(version),
            ApiKey::WriteTxnMarkers => WriteTxnMarkersRequest::header_version(version),
            ApiKey::TxnOffsetCommit => TxnOffsetCommitRequest::header_version(version),
            ApiKey::DescribeAcls => DescribeAclsRequest::header_version(version),
            ApiKey::CreateAcls => CreateAclsRequest::header_version(version),
            ApiKey::DeleteAcls => DeleteAclsRequest::header_version(version),
            ApiKey::DescribeConfigs => DescribeConfigsRequest::header_version(version),
            ApiKey::AlterConfigs => AlterConfigsRequest::header_version(version),
            ApiKey::AlterReplicaLogDirs => AlterReplicaLogDirsRequest::header_version(version),
            ApiKey::DescribeLogDirs => DescribeLogDirsRequest::header_version(version),
            ApiKey::SaslAuthenticate => SaslAuthenticateRequest::header_version(version),
            ApiKey::CreatePartitions => CreatePartitionsRequest::header_version(version),
            ApiKey::CreateDelegationToken => CreateDelegationTokenRequest::header_version(version),
            ApiKey::RenewDelegationToken => RenewDelegationTokenRequest::header_version(version),
            ApiKey::ExpireDelegationToken => ExpireDelegationTokenRequest::header_version(version),
            ApiKey::DescribeDelegationToken => {
                DescribeDelegationTokenRequest::header_version(version)
            }
            ApiKey::DeleteGroups => DeleteGroupsRequest::header_version(version),
            ApiKey::ElectLeaders => ElectLeadersRequest::header_version(version),
            ApiKey::IncrementalAlterConfigs => {
                IncrementalAlterConfigsRequest::header_version(version)
            }
            ApiKey::AlterPartitionReassignments => {
                AlterPartitionReassignmentsRequest::header_version(version)
            }
            ApiKey::ListPartitionReassignments => {
                ListPartitionReassignmentsRequest::header_version(version)
            }
            ApiKey::OffsetDelete => OffsetDeleteRequest::header_version(version),
            ApiKey::DescribeClientQuotas => DescribeClientQuotasRequest::header_version(version),
            ApiKey::AlterClientQuotas => AlterClientQuotasRequest::header_version(version),
            ApiKey::DescribeUserScramCredentials => {
                DescribeUserScramCredentialsRequest::header_version(version)
            }
            ApiKey::AlterUserScramCredentials => {
                AlterUserScramCredentialsRequest::header_version(version)
            }
            ApiKey::Vote => VoteRequest::header_version(version),
            ApiKey::BeginQuorumEpoch => BeginQuorumEpochRequest::header_version(version),
            ApiKey::EndQuorumEpoch => EndQuorumEpochRequest::header_version(version),
            ApiKey::DescribeQuorum => DescribeQuorumRequest::header_version(version),
            ApiKey::AlterPartition => AlterPartitionRequest::header_version(version),
            ApiKey::UpdateFeatures => UpdateFeaturesRequest::header_version(version),
            ApiKey::Envelope => EnvelopeRequest::header_version(version),
            ApiKey::FetchSnapshot => FetchSnapshotRequest::header_version(version),
            ApiKey::DescribeCluster => DescribeClusterRequest::header_version(version),
            ApiKey::DescribeProducers => DescribeProducersRequest::header_version(version),
            ApiKey::BrokerRegistration => BrokerRegistrationRequest::header_version(version),
            ApiKey::BrokerHeartbeat => BrokerHeartbeatRequest::header_version(version),
            ApiKey::UnregisterBroker => UnregisterBrokerRequest::header_version(version),
            ApiKey::DescribeTransactions => DescribeTransactionsRequest::header_version(version),
            ApiKey::ListTransactions => ListTransactionsRequest::header_version(version),
            ApiKey::AllocateProducerIds => AllocateProducerIdsRequest::header_version(version),
            ApiKey::ConsumerGroupHeartbeat => {
                ConsumerGroupHeartbeatRequest::header_version(version)
            }
            ApiKey::ConsumerGroupDescribe => ConsumerGroupDescribeRequest::header_version(version),
            ApiKey::ControllerRegistration => {
                ControllerRegistrationRequest::header_version(version)
            }
            ApiKey::GetTelemetrySubscriptions => {
                GetTelemetrySubscriptionsRequest::header_version(version)
            }
            ApiKey::PushTelemetry => PushTelemetryRequest::header_version(version),
            ApiKey::AssignReplicasToDirs => AssignReplicasToDirsRequest::header_version(version),
            ApiKey::ListConfigResources => ListConfigResourcesRequest::header_version(version),
            ApiKey::DescribeTopicPartitions => {
                DescribeTopicPartitionsRequest::header_version(version)
            }
            ApiKey::ShareGroupHeartbeat => ShareGroupHeartbeatRequest::header_version(version),
            ApiKey::ShareGroupDescribe => ShareGroupDescribeRequest::header_version(version),
            ApiKey::ShareFetch => ShareFetchRequest::header_version(version),
            ApiKey::ShareAcknowledge => ShareAcknowledgeRequest::header_version(version),
            ApiKey::AddRaftVoter => AddRaftVoterRequest::header_version(version),
            ApiKey::RemoveRaftVoter => RemoveRaftVoterRequest::header_version(version),
            ApiKey::UpdateRaftVoter => UpdateRaftVoterRequest::header_version(version),
            ApiKey::InitializeShareGroupState => {
                InitializeShareGroupStateRequest::header_version(version)
            }
            ApiKey::ReadShareGroupState => ReadShareGroupStateRequest::header_version(version),
            ApiKey::WriteShareGroupState => WriteShareGroupStateRequest::header_version(version),
            ApiKey::DeleteShareGroupState => DeleteShareGroupStateRequest::header_version(version),
            ApiKey::ReadShareGroupStateSummary => {
                ReadShareGroupStateSummaryRequest::header_version(version)
            }
            ApiKey::DescribeShareGroupOffsets => {
                DescribeShareGroupOffsetsRequest::header_version(version)
            }
            ApiKey::AlterShareGroupOffsets => {
                AlterShareGroupOffsetsRequest::header_version(version)
            }
            ApiKey::DeleteShareGroupOffsets => {
                DeleteShareGroupOffsetsRequest::header_version(version)
            }
        }
    }
    /// Get the version of response header that needs to be prepended to this message
    pub fn response_header_version(&self, version: i16) -> i16 {
        match self {
            ApiKey::Produce => ProduceResponse::header_version(version),
            ApiKey::Fetch => FetchResponse::header_version(version),
            ApiKey::ListOffsets => ListOffsetsResponse::header_version(version),
            ApiKey::Metadata => MetadataResponse::header_version(version),
            ApiKey::OffsetCommit => OffsetCommitResponse::header_version(version),
            ApiKey::OffsetFetch => OffsetFetchResponse::header_version(version),
            ApiKey::FindCoordinator => FindCoordinatorResponse::header_version(version),
            ApiKey::JoinGroup => JoinGroupResponse::header_version(version),
            ApiKey::Heartbeat => HeartbeatResponse::header_version(version),
            ApiKey::LeaveGroup => LeaveGroupResponse::header_version(version),
            ApiKey::SyncGroup => SyncGroupResponse::header_version(version),
            ApiKey::DescribeGroups => DescribeGroupsResponse::header_version(version),
            ApiKey::ListGroups => ListGroupsResponse::header_version(version),
            ApiKey::SaslHandshake => SaslHandshakeResponse::header_version(version),
            ApiKey::ApiVersions => ApiVersionsResponse::header_version(version),
            ApiKey::CreateTopics => CreateTopicsResponse::header_version(version),
            ApiKey::DeleteTopics => DeleteTopicsResponse::header_version(version),
            ApiKey::DeleteRecords => DeleteRecordsResponse::header_version(version),
            ApiKey::InitProducerId => InitProducerIdResponse::header_version(version),
            ApiKey::OffsetForLeaderEpoch => OffsetForLeaderEpochResponse::header_version(version),
            ApiKey::AddPartitionsToTxn => AddPartitionsToTxnResponse::header_version(version),
            ApiKey::AddOffsetsToTxn => AddOffsetsToTxnResponse::header_version(version),
            ApiKey::EndTxn => EndTxnResponse::header_version(version),
            ApiKey::WriteTxnMarkers => WriteTxnMarkersResponse::header_version(version),
            ApiKey::TxnOffsetCommit => TxnOffsetCommitResponse::header_version(version),
            ApiKey::DescribeAcls => DescribeAclsResponse::header_version(version),
            ApiKey::CreateAcls => CreateAclsResponse::header_version(version),
            ApiKey::DeleteAcls => DeleteAclsResponse::header_version(version),
            ApiKey::DescribeConfigs => DescribeConfigsResponse::header_version(version),
            ApiKey::AlterConfigs => AlterConfigsResponse::header_version(version),
            ApiKey::AlterReplicaLogDirs => AlterReplicaLogDirsResponse::header_version(version),
            ApiKey::DescribeLogDirs => DescribeLogDirsResponse::header_version(version),
            ApiKey::SaslAuthenticate => SaslAuthenticateResponse::header_version(version),
            ApiKey::CreatePartitions => CreatePartitionsResponse::header_version(version),
            ApiKey::CreateDelegationToken => CreateDelegationTokenResponse::header_version(version),
            ApiKey::RenewDelegationToken => RenewDelegationTokenResponse::header_version(version),
            ApiKey::ExpireDelegationToken => ExpireDelegationTokenResponse::header_version(version),
            ApiKey::DescribeDelegationToken => {
                DescribeDelegationTokenResponse::header_version(version)
            }
            ApiKey::DeleteGroups => DeleteGroupsResponse::header_version(version),
            ApiKey::ElectLeaders => ElectLeadersResponse::header_version(version),
            ApiKey::IncrementalAlterConfigs => {
                IncrementalAlterConfigsResponse::header_version(version)
            }
            ApiKey::AlterPartitionReassignments => {
                AlterPartitionReassignmentsResponse::header_version(version)
            }
            ApiKey::ListPartitionReassignments => {
                ListPartitionReassignmentsResponse::header_version(version)
            }
            ApiKey::OffsetDelete => OffsetDeleteResponse::header_version(version),
            ApiKey::DescribeClientQuotas => DescribeClientQuotasResponse::header_version(version),
            ApiKey::AlterClientQuotas => AlterClientQuotasResponse::header_version(version),
            ApiKey::DescribeUserScramCredentials => {
                DescribeUserScramCredentialsResponse::header_version(version)
            }
            ApiKey::AlterUserScramCredentials => {
                AlterUserScramCredentialsResponse::header_version(version)
            }
            ApiKey::Vote => VoteResponse::header_version(version),
            ApiKey::BeginQuorumEpoch => BeginQuorumEpochResponse::header_version(version),
            ApiKey::EndQuorumEpoch => EndQuorumEpochResponse::header_version(version),
            ApiKey::DescribeQuorum => DescribeQuorumResponse::header_version(version),
            ApiKey::AlterPartition => AlterPartitionResponse::header_version(version),
            ApiKey::UpdateFeatures => UpdateFeaturesResponse::header_version(version),
            ApiKey::Envelope => EnvelopeResponse::header_version(version),
            ApiKey::FetchSnapshot => FetchSnapshotResponse::header_version(version),
            ApiKey::DescribeCluster => DescribeClusterResponse::header_version(version),
            ApiKey::DescribeProducers => DescribeProducersResponse::header_version(version),
            ApiKey::BrokerRegistration => BrokerRegistrationResponse::header_version(version),
            ApiKey::BrokerHeartbeat => BrokerHeartbeatResponse::header_version(version),
            ApiKey::UnregisterBroker => UnregisterBrokerResponse::header_version(version),
            ApiKey::DescribeTransactions => DescribeTransactionsResponse::header_version(version),
            ApiKey::ListTransactions => ListTransactionsResponse::header_version(version),
            ApiKey::AllocateProducerIds => AllocateProducerIdsResponse::header_version(version),
            ApiKey::ConsumerGroupHeartbeat => {
                ConsumerGroupHeartbeatResponse::header_version(version)
            }
            ApiKey::ConsumerGroupDescribe => ConsumerGroupDescribeResponse::header_version(version),
            ApiKey::ControllerRegistration => {
                ControllerRegistrationResponse::header_version(version)
            }
            ApiKey::GetTelemetrySubscriptions => {
                GetTelemetrySubscriptionsResponse::header_version(version)
            }
            ApiKey::PushTelemetry => PushTelemetryResponse::header_version(version),
            ApiKey::AssignReplicasToDirs => AssignReplicasToDirsResponse::header_version(version),
            ApiKey::ListConfigResources => ListConfigResourcesResponse::header_version(version),
            ApiKey::DescribeTopicPartitions => {
                DescribeTopicPartitionsResponse::header_version(version)
            }
            ApiKey::ShareGroupHeartbeat => ShareGroupHeartbeatResponse::header_version(version),
            ApiKey::ShareGroupDescribe => ShareGroupDescribeResponse::header_version(version),
            ApiKey::ShareFetch => ShareFetchResponse::header_version(version),
            ApiKey::ShareAcknowledge => ShareAcknowledgeResponse::header_version(version),
            ApiKey::AddRaftVoter => AddRaftVoterResponse::header_version(version),
            ApiKey::RemoveRaftVoter => RemoveRaftVoterResponse::header_version(version),
            ApiKey::UpdateRaftVoter => UpdateRaftVoterResponse::header_version(version),
            ApiKey::InitializeShareGroupState => {
                InitializeShareGroupStateResponse::header_version(version)
            }
            ApiKey::ReadShareGroupState => ReadShareGroupStateResponse::header_version(version),
            ApiKey::WriteShareGroupState => WriteShareGroupStateResponse::header_version(version),
            ApiKey::DeleteShareGroupState => DeleteShareGroupStateResponse::header_version(version),
            ApiKey::ReadShareGroupStateSummary => {
                ReadShareGroupStateSummaryResponse::header_version(version)
            }
            ApiKey::DescribeShareGroupOffsets => {
                DescribeShareGroupOffsetsResponse::header_version(version)
            }
            ApiKey::AlterShareGroupOffsets => {
                AlterShareGroupOffsetsResponse::header_version(version)
            }
            ApiKey::DeleteShareGroupOffsets => {
                DeleteShareGroupOffsetsResponse::header_version(version)
            }
        }
    }
    /// Returns the valid versions that can be used with this ApiKey
    pub fn valid_versions(&self) -> VersionRange {
        match self {
            ApiKey::Produce => VersionRange { min: 3, max: 13 },
            ApiKey::Fetch => VersionRange { min: 4, max: 18 },
            ApiKey::ListOffsets => VersionRange { min: 1, max: 10 },
            ApiKey::Metadata => VersionRange { min: 0, max: 13 },
            ApiKey::OffsetCommit => VersionRange { min: 2, max: 10 },
            ApiKey::OffsetFetch => VersionRange { min: 1, max: 10 },
            ApiKey::FindCoordinator => VersionRange { min: 0, max: 6 },
            ApiKey::JoinGroup => VersionRange { min: 0, max: 9 },
            ApiKey::Heartbeat => VersionRange { min: 0, max: 4 },
            ApiKey::LeaveGroup => VersionRange { min: 0, max: 5 },
            ApiKey::SyncGroup => VersionRange { min: 0, max: 5 },
            ApiKey::DescribeGroups => VersionRange { min: 0, max: 6 },
            ApiKey::ListGroups => VersionRange { min: 0, max: 5 },
            ApiKey::SaslHandshake => VersionRange { min: 0, max: 1 },
            ApiKey::ApiVersions => VersionRange { min: 0, max: 4 },
            ApiKey::CreateTopics => VersionRange { min: 2, max: 7 },
            ApiKey::DeleteTopics => VersionRange { min: 1, max: 6 },
            ApiKey::DeleteRecords => VersionRange { min: 0, max: 2 },
            ApiKey::InitProducerId => VersionRange { min: 0, max: 6 },
            ApiKey::OffsetForLeaderEpoch => VersionRange { min: 2, max: 4 },
            ApiKey::AddPartitionsToTxn => VersionRange { min: 0, max: 5 },
            ApiKey::AddOffsetsToTxn => VersionRange { min: 0, max: 4 },
            ApiKey::EndTxn => VersionRange { min: 0, max: 5 },
            ApiKey::WriteTxnMarkers => VersionRange { min: 1, max: 1 },
            ApiKey::TxnOffsetCommit => VersionRange { min: 0, max: 5 },
            ApiKey::DescribeAcls => VersionRange { min: 1, max: 3 },
            ApiKey::CreateAcls => VersionRange { min: 1, max: 3 },
            ApiKey::DeleteAcls => VersionRange { min: 1, max: 3 },
            ApiKey::DescribeConfigs => VersionRange { min: 1, max: 4 },
            ApiKey::AlterConfigs => VersionRange { min: 0, max: 2 },
            ApiKey::AlterReplicaLogDirs => VersionRange { min: 1, max: 2 },
            ApiKey::DescribeLogDirs => VersionRange { min: 1, max: 4 },
            ApiKey::SaslAuthenticate => VersionRange { min: 0, max: 2 },
            ApiKey::CreatePartitions => VersionRange { min: 0, max: 3 },
            ApiKey::CreateDelegationToken => VersionRange { min: 1, max: 3 },
            ApiKey::RenewDelegationToken => VersionRange { min: 1, max: 2 },
            ApiKey::ExpireDelegationToken => VersionRange { min: 1, max: 2 },
            ApiKey::DescribeDelegationToken => VersionRange { min: 1, max: 3 },
            ApiKey::DeleteGroups => VersionRange { min: 0, max: 2 },
            ApiKey::ElectLeaders => VersionRange { min: 0, max: 2 },
            ApiKey::IncrementalAlterConfigs => VersionRange { min: 0, max: 1 },
            ApiKey::AlterPartitionReassignments => VersionRange { min: 0, max: 1 },
            ApiKey::ListPartitionReassignments => VersionRange { min: 0, max: 0 },
            ApiKey::OffsetDelete => VersionRange { min: 0, max: 0 },
            ApiKey::DescribeClientQuotas => VersionRange { min: 0, max: 1 },
            ApiKey::AlterClientQuotas => VersionRange { min: 0, max: 1 },
            ApiKey::DescribeUserScramCredentials => VersionRange { min: 0, max: 0 },
            ApiKey::AlterUserScramCredentials => VersionRange { min: 0, max: 0 },
            ApiKey::Vote => VersionRange { min: 0, max: 2 },
            ApiKey::BeginQuorumEpoch => VersionRange { min: 0, max: 1 },
            ApiKey::EndQuorumEpoch => VersionRange { min: 0, max: 1 },
            ApiKey::DescribeQuorum => VersionRange { min: 0, max: 2 },
            ApiKey::AlterPartition => VersionRange { min: 2, max: 3 },
            ApiKey::UpdateFeatures => VersionRange { min: 0, max: 2 },
            ApiKey::Envelope => VersionRange { min: 0, max: 0 },
            ApiKey::FetchSnapshot => VersionRange { min: 0, max: 1 },
            ApiKey::DescribeCluster => VersionRange { min: 0, max: 2 },
            ApiKey::DescribeProducers => VersionRange { min: 0, max: 0 },
            ApiKey::BrokerRegistration => VersionRange { min: 0, max: 4 },
            ApiKey::BrokerHeartbeat => VersionRange { min: 0, max: 1 },
            ApiKey::UnregisterBroker => VersionRange { min: 0, max: 0 },
            ApiKey::DescribeTransactions => VersionRange { min: 0, max: 0 },
            ApiKey::ListTransactions => VersionRange { min: 0, max: 2 },
            ApiKey::AllocateProducerIds => VersionRange { min: 0, max: 0 },
            ApiKey::ConsumerGroupHeartbeat => VersionRange { min: 0, max: 1 },
            ApiKey::ConsumerGroupDescribe => VersionRange { min: 0, max: 1 },
            ApiKey::ControllerRegistration => VersionRange { min: 0, max: 0 },
            ApiKey::GetTelemetrySubscriptions => VersionRange { min: 0, max: 0 },
            ApiKey::PushTelemetry => VersionRange { min: 0, max: 0 },
            ApiKey::AssignReplicasToDirs => VersionRange { min: 0, max: 0 },
            ApiKey::ListConfigResources => VersionRange { min: 0, max: 1 },
            ApiKey::DescribeTopicPartitions => VersionRange { min: 0, max: 0 },
            ApiKey::ShareGroupHeartbeat => VersionRange { min: 1, max: 1 },
            ApiKey::ShareGroupDescribe => VersionRange { min: 1, max: 1 },
            ApiKey::ShareFetch => VersionRange { min: 1, max: 1 },
            ApiKey::ShareAcknowledge => VersionRange { min: 1, max: 1 },
            ApiKey::AddRaftVoter => VersionRange { min: 0, max: 0 },
            ApiKey::RemoveRaftVoter => VersionRange { min: 0, max: 0 },
            ApiKey::UpdateRaftVoter => VersionRange { min: 0, max: 0 },
            ApiKey::InitializeShareGroupState => VersionRange { min: 0, max: 0 },
            ApiKey::ReadShareGroupState => VersionRange { min: 0, max: 0 },
            ApiKey::WriteShareGroupState => VersionRange { min: 0, max: 0 },
            ApiKey::DeleteShareGroupState => VersionRange { min: 0, max: 0 },
            ApiKey::ReadShareGroupStateSummary => VersionRange { min: 0, max: 0 },
            ApiKey::DescribeShareGroupOffsets => VersionRange { min: 0, max: 0 },
            ApiKey::AlterShareGroupOffsets => VersionRange { min: 0, max: 0 },
            ApiKey::DeleteShareGroupOffsets => VersionRange { min: 0, max: 0 },
        }
    }

    /// Iterate through every ApiKey variant in the order of the internal code.
    pub fn iter() -> impl Iterator<Item = ApiKey> {
        (0..=92).filter_map(|i| ApiKey::try_from(i).ok())
    }
}
impl TryFrom<i16> for ApiKey {
    type Error = ();

    fn try_from(v: i16) -> std::result::Result<Self, Self::Error> {
        match v {
            x if x == ApiKey::Produce as i16 => Ok(ApiKey::Produce),
            x if x == ApiKey::Fetch as i16 => Ok(ApiKey::Fetch),
            x if x == ApiKey::ListOffsets as i16 => Ok(ApiKey::ListOffsets),
            x if x == ApiKey::Metadata as i16 => Ok(ApiKey::Metadata),
            x if x == ApiKey::OffsetCommit as i16 => Ok(ApiKey::OffsetCommit),
            x if x == ApiKey::OffsetFetch as i16 => Ok(ApiKey::OffsetFetch),
            x if x == ApiKey::FindCoordinator as i16 => Ok(ApiKey::FindCoordinator),
            x if x == ApiKey::JoinGroup as i16 => Ok(ApiKey::JoinGroup),
            x if x == ApiKey::Heartbeat as i16 => Ok(ApiKey::Heartbeat),
            x if x == ApiKey::LeaveGroup as i16 => Ok(ApiKey::LeaveGroup),
            x if x == ApiKey::SyncGroup as i16 => Ok(ApiKey::SyncGroup),
            x if x == ApiKey::DescribeGroups as i16 => Ok(ApiKey::DescribeGroups),
            x if x == ApiKey::ListGroups as i16 => Ok(ApiKey::ListGroups),
            x if x == ApiKey::SaslHandshake as i16 => Ok(ApiKey::SaslHandshake),
            x if x == ApiKey::ApiVersions as i16 => Ok(ApiKey::ApiVersions),
            x if x == ApiKey::CreateTopics as i16 => Ok(ApiKey::CreateTopics),
            x if x == ApiKey::DeleteTopics as i16 => Ok(ApiKey::DeleteTopics),
            x if x == ApiKey::DeleteRecords as i16 => Ok(ApiKey::DeleteRecords),
            x if x == ApiKey::InitProducerId as i16 => Ok(ApiKey::InitProducerId),
            x if x == ApiKey::OffsetForLeaderEpoch as i16 => Ok(ApiKey::OffsetForLeaderEpoch),
            x if x == ApiKey::AddPartitionsToTxn as i16 => Ok(ApiKey::AddPartitionsToTxn),
            x if x == ApiKey::AddOffsetsToTxn as i16 => Ok(ApiKey::AddOffsetsToTxn),
            x if x == ApiKey::EndTxn as i16 => Ok(ApiKey::EndTxn),
            x if x == ApiKey::WriteTxnMarkers as i16 => Ok(ApiKey::WriteTxnMarkers),
            x if x == ApiKey::TxnOffsetCommit as i16 => Ok(ApiKey::TxnOffsetCommit),
            x if x == ApiKey::DescribeAcls as i16 => Ok(ApiKey::DescribeAcls),
            x if x == ApiKey::CreateAcls as i16 => Ok(ApiKey::CreateAcls),
            x if x == ApiKey::DeleteAcls as i16 => Ok(ApiKey::DeleteAcls),
            x if x == ApiKey::DescribeConfigs as i16 => Ok(ApiKey::DescribeConfigs),
            x if x == ApiKey::AlterConfigs as i16 => Ok(ApiKey::AlterConfigs),
            x if x == ApiKey::AlterReplicaLogDirs as i16 => Ok(ApiKey::AlterReplicaLogDirs),
            x if x == ApiKey::DescribeLogDirs as i16 => Ok(ApiKey::DescribeLogDirs),
            x if x == ApiKey::SaslAuthenticate as i16 => Ok(ApiKey::SaslAuthenticate),
            x if x == ApiKey::CreatePartitions as i16 => Ok(ApiKey::CreatePartitions),
            x if x == ApiKey::CreateDelegationToken as i16 => Ok(ApiKey::CreateDelegationToken),
            x if x == ApiKey::RenewDelegationToken as i16 => Ok(ApiKey::RenewDelegationToken),
            x if x == ApiKey::ExpireDelegationToken as i16 => Ok(ApiKey::ExpireDelegationToken),
            x if x == ApiKey::DescribeDelegationToken as i16 => Ok(ApiKey::DescribeDelegationToken),
            x if x == ApiKey::DeleteGroups as i16 => Ok(ApiKey::DeleteGroups),
            x if x == ApiKey::ElectLeaders as i16 => Ok(ApiKey::ElectLeaders),
            x if x == ApiKey::IncrementalAlterConfigs as i16 => Ok(ApiKey::IncrementalAlterConfigs),
            x if x == ApiKey::AlterPartitionReassignments as i16 => {
                Ok(ApiKey::AlterPartitionReassignments)
            }
            x if x == ApiKey::ListPartitionReassignments as i16 => {
                Ok(ApiKey::ListPartitionReassignments)
            }
            x if x == ApiKey::OffsetDelete as i16 => Ok(ApiKey::OffsetDelete),
            x if x == ApiKey::DescribeClientQuotas as i16 => Ok(ApiKey::DescribeClientQuotas),
            x if x == ApiKey::AlterClientQuotas as i16 => Ok(ApiKey::AlterClientQuotas),
            x if x == ApiKey::DescribeUserScramCredentials as i16 => {
                Ok(ApiKey::DescribeUserScramCredentials)
            }
            x if x == ApiKey::AlterUserScramCredentials as i16 => {
                Ok(ApiKey::AlterUserScramCredentials)
            }
            x if x == ApiKey::Vote as i16 => Ok(ApiKey::Vote),
            x if x == ApiKey::BeginQuorumEpoch as i16 => Ok(ApiKey::BeginQuorumEpoch),
            x if x == ApiKey::EndQuorumEpoch as i16 => Ok(ApiKey::EndQuorumEpoch),
            x if x == ApiKey::DescribeQuorum as i16 => Ok(ApiKey::DescribeQuorum),
            x if x == ApiKey::AlterPartition as i16 => Ok(ApiKey::AlterPartition),
            x if x == ApiKey::UpdateFeatures as i16 => Ok(ApiKey::UpdateFeatures),
            x if x == ApiKey::Envelope as i16 => Ok(ApiKey::Envelope),
            x if x == ApiKey::FetchSnapshot as i16 => Ok(ApiKey::FetchSnapshot),
            x if x == ApiKey::DescribeCluster as i16 => Ok(ApiKey::DescribeCluster),
            x if x == ApiKey::DescribeProducers as i16 => Ok(ApiKey::DescribeProducers),
            x if x == ApiKey::BrokerRegistration as i16 => Ok(ApiKey::BrokerRegistration),
            x if x == ApiKey::BrokerHeartbeat as i16 => Ok(ApiKey::BrokerHeartbeat),
            x if x == ApiKey::UnregisterBroker as i16 => Ok(ApiKey::UnregisterBroker),
            x if x == ApiKey::DescribeTransactions as i16 => Ok(ApiKey::DescribeTransactions),
            x if x == ApiKey::ListTransactions as i16 => Ok(ApiKey::ListTransactions),
            x if x == ApiKey::AllocateProducerIds as i16 => Ok(ApiKey::AllocateProducerIds),
            x if x == ApiKey::ConsumerGroupHeartbeat as i16 => Ok(ApiKey::ConsumerGroupHeartbeat),
            x if x == ApiKey::ConsumerGroupDescribe as i16 => Ok(ApiKey::ConsumerGroupDescribe),
            x if x == ApiKey::ControllerRegistration as i16 => Ok(ApiKey::ControllerRegistration),
            x if x == ApiKey::GetTelemetrySubscriptions as i16 => {
                Ok(ApiKey::GetTelemetrySubscriptions)
            }
            x if x == ApiKey::PushTelemetry as i16 => Ok(ApiKey::PushTelemetry),
            x if x == ApiKey::AssignReplicasToDirs as i16 => Ok(ApiKey::AssignReplicasToDirs),
            x if x == ApiKey::ListConfigResources as i16 => Ok(ApiKey::ListConfigResources),
            x if x == ApiKey::DescribeTopicPartitions as i16 => Ok(ApiKey::DescribeTopicPartitions),
            x if x == ApiKey::ShareGroupHeartbeat as i16 => Ok(ApiKey::ShareGroupHeartbeat),
            x if x == ApiKey::ShareGroupDescribe as i16 => Ok(ApiKey::ShareGroupDescribe),
            x if x == ApiKey::ShareFetch as i16 => Ok(ApiKey::ShareFetch),
            x if x == ApiKey::ShareAcknowledge as i16 => Ok(ApiKey::ShareAcknowledge),
            x if x == ApiKey::AddRaftVoter as i16 => Ok(ApiKey::AddRaftVoter),
            x if x == ApiKey::RemoveRaftVoter as i16 => Ok(ApiKey::RemoveRaftVoter),
            x if x == ApiKey::UpdateRaftVoter as i16 => Ok(ApiKey::UpdateRaftVoter),
            x if x == ApiKey::InitializeShareGroupState as i16 => {
                Ok(ApiKey::InitializeShareGroupState)
            }
            x if x == ApiKey::ReadShareGroupState as i16 => Ok(ApiKey::ReadShareGroupState),
            x if x == ApiKey::WriteShareGroupState as i16 => Ok(ApiKey::WriteShareGroupState),
            x if x == ApiKey::DeleteShareGroupState as i16 => Ok(ApiKey::DeleteShareGroupState),
            x if x == ApiKey::ReadShareGroupStateSummary as i16 => {
                Ok(ApiKey::ReadShareGroupStateSummary)
            }
            x if x == ApiKey::DescribeShareGroupOffsets as i16 => {
                Ok(ApiKey::DescribeShareGroupOffsets)
            }
            x if x == ApiKey::AlterShareGroupOffsets as i16 => Ok(ApiKey::AlterShareGroupOffsets),
            x if x == ApiKey::DeleteShareGroupOffsets as i16 => Ok(ApiKey::DeleteShareGroupOffsets),
            _ => Err(()),
        }
    }
}

/// Wrapping enum for all requests in the Kafka protocol.
#[cfg(feature = "messages_enums")]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum RequestKind {
    /// ProduceRequest,
    Produce(ProduceRequest),
    /// FetchRequest,
    Fetch(FetchRequest),
    /// ListOffsetsRequest,
    ListOffsets(ListOffsetsRequest),
    /// MetadataRequest,
    Metadata(MetadataRequest),
    /// OffsetCommitRequest,
    OffsetCommit(OffsetCommitRequest),
    /// OffsetFetchRequest,
    OffsetFetch(OffsetFetchRequest),
    /// FindCoordinatorRequest,
    FindCoordinator(FindCoordinatorRequest),
    /// JoinGroupRequest,
    JoinGroup(JoinGroupRequest),
    /// HeartbeatRequest,
    Heartbeat(HeartbeatRequest),
    /// LeaveGroupRequest,
    LeaveGroup(LeaveGroupRequest),
    /// SyncGroupRequest,
    SyncGroup(SyncGroupRequest),
    /// DescribeGroupsRequest,
    DescribeGroups(DescribeGroupsRequest),
    /// ListGroupsRequest,
    ListGroups(ListGroupsRequest),
    /// SaslHandshakeRequest,
    SaslHandshake(SaslHandshakeRequest),
    /// ApiVersionsRequest,
    ApiVersions(ApiVersionsRequest),
    /// CreateTopicsRequest,
    CreateTopics(CreateTopicsRequest),
    /// DeleteTopicsRequest,
    DeleteTopics(DeleteTopicsRequest),
    /// DeleteRecordsRequest,
    DeleteRecords(DeleteRecordsRequest),
    /// InitProducerIdRequest,
    InitProducerId(InitProducerIdRequest),
    /// OffsetForLeaderEpochRequest,
    OffsetForLeaderEpoch(OffsetForLeaderEpochRequest),
    /// AddPartitionsToTxnRequest,
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    /// AddOffsetsToTxnRequest,
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    /// EndTxnRequest,
    EndTxn(EndTxnRequest),
    /// WriteTxnMarkersRequest,
    WriteTxnMarkers(WriteTxnMarkersRequest),
    /// TxnOffsetCommitRequest,
    TxnOffsetCommit(TxnOffsetCommitRequest),
    /// DescribeAclsRequest,
    DescribeAcls(DescribeAclsRequest),
    /// CreateAclsRequest,
    CreateAcls(CreateAclsRequest),
    /// DeleteAclsRequest,
    DeleteAcls(DeleteAclsRequest),
    /// DescribeConfigsRequest,
    DescribeConfigs(DescribeConfigsRequest),
    /// AlterConfigsRequest,
    AlterConfigs(AlterConfigsRequest),
    /// AlterReplicaLogDirsRequest,
    AlterReplicaLogDirs(AlterReplicaLogDirsRequest),
    /// DescribeLogDirsRequest,
    DescribeLogDirs(DescribeLogDirsRequest),
    /// SaslAuthenticateRequest,
    SaslAuthenticate(SaslAuthenticateRequest),
    /// CreatePartitionsRequest,
    CreatePartitions(CreatePartitionsRequest),
    /// CreateDelegationTokenRequest,
    CreateDelegationToken(CreateDelegationTokenRequest),
    /// RenewDelegationTokenRequest,
    RenewDelegationToken(RenewDelegationTokenRequest),
    /// ExpireDelegationTokenRequest,
    ExpireDelegationToken(ExpireDelegationTokenRequest),
    /// DescribeDelegationTokenRequest,
    DescribeDelegationToken(DescribeDelegationTokenRequest),
    /// DeleteGroupsRequest,
    DeleteGroups(DeleteGroupsRequest),
    /// ElectLeadersRequest,
    ElectLeaders(ElectLeadersRequest),
    /// IncrementalAlterConfigsRequest,
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    /// AlterPartitionReassignmentsRequest,
    AlterPartitionReassignments(AlterPartitionReassignmentsRequest),
    /// ListPartitionReassignmentsRequest,
    ListPartitionReassignments(ListPartitionReassignmentsRequest),
    /// OffsetDeleteRequest,
    OffsetDelete(OffsetDeleteRequest),
    /// DescribeClientQuotasRequest,
    DescribeClientQuotas(DescribeClientQuotasRequest),
    /// AlterClientQuotasRequest,
    AlterClientQuotas(AlterClientQuotasRequest),
    /// DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentials(DescribeUserScramCredentialsRequest),
    /// AlterUserScramCredentialsRequest,
    AlterUserScramCredentials(AlterUserScramCredentialsRequest),
    /// VoteRequest,
    Vote(VoteRequest),
    /// BeginQuorumEpochRequest,
    BeginQuorumEpoch(BeginQuorumEpochRequest),
    /// EndQuorumEpochRequest,
    EndQuorumEpoch(EndQuorumEpochRequest),
    /// DescribeQuorumRequest,
    DescribeQuorum(DescribeQuorumRequest),
    /// AlterPartitionRequest,
    AlterPartition(AlterPartitionRequest),
    /// UpdateFeaturesRequest,
    UpdateFeatures(UpdateFeaturesRequest),
    /// EnvelopeRequest,
    Envelope(EnvelopeRequest),
    /// FetchSnapshotRequest,
    FetchSnapshot(FetchSnapshotRequest),
    /// DescribeClusterRequest,
    DescribeCluster(DescribeClusterRequest),
    /// DescribeProducersRequest,
    DescribeProducers(DescribeProducersRequest),
    /// BrokerRegistrationRequest,
    BrokerRegistration(BrokerRegistrationRequest),
    /// BrokerHeartbeatRequest,
    BrokerHeartbeat(BrokerHeartbeatRequest),
    /// UnregisterBrokerRequest,
    UnregisterBroker(UnregisterBrokerRequest),
    /// DescribeTransactionsRequest,
    DescribeTransactions(DescribeTransactionsRequest),
    /// ListTransactionsRequest,
    ListTransactions(ListTransactionsRequest),
    /// AllocateProducerIdsRequest,
    AllocateProducerIds(AllocateProducerIdsRequest),
    /// ConsumerGroupHeartbeatRequest,
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatRequest),
    /// ConsumerGroupDescribeRequest,
    ConsumerGroupDescribe(ConsumerGroupDescribeRequest),
    /// ControllerRegistrationRequest,
    ControllerRegistration(ControllerRegistrationRequest),
    /// GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsRequest),
    /// PushTelemetryRequest,
    PushTelemetry(PushTelemetryRequest),
    /// AssignReplicasToDirsRequest,
    AssignReplicasToDirs(AssignReplicasToDirsRequest),
    /// ListConfigResourcesRequest,
    ListConfigResources(ListConfigResourcesRequest),
    /// DescribeTopicPartitionsRequest,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    /// ShareGroupHeartbeatRequest,
    ShareGroupHeartbeat(ShareGroupHeartbeatRequest),
    /// ShareGroupDescribeRequest,
    ShareGroupDescribe(ShareGroupDescribeRequest),
    /// ShareFetchRequest,
    ShareFetch(ShareFetchRequest),
    /// ShareAcknowledgeRequest,
    ShareAcknowledge(ShareAcknowledgeRequest),
    /// AddRaftVoterRequest,
    AddRaftVoter(AddRaftVoterRequest),
    /// RemoveRaftVoterRequest,
    RemoveRaftVoter(RemoveRaftVoterRequest),
    /// UpdateRaftVoterRequest,
    UpdateRaftVoter(UpdateRaftVoterRequest),
    /// InitializeShareGroupStateRequest,
    InitializeShareGroupState(InitializeShareGroupStateRequest),
    /// ReadShareGroupStateRequest,
    ReadShareGroupState(ReadShareGroupStateRequest),
    /// WriteShareGroupStateRequest,
    WriteShareGroupState(WriteShareGroupStateRequest),
    /// DeleteShareGroupStateRequest,
    DeleteShareGroupState(DeleteShareGroupStateRequest),
    /// ReadShareGroupStateSummaryRequest,
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryRequest),
    /// DescribeShareGroupOffsetsRequest,
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsRequest),
    /// AlterShareGroupOffsetsRequest,
    AlterShareGroupOffsets(AlterShareGroupOffsetsRequest),
    /// DeleteShareGroupOffsetsRequest,
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsRequest),
}

#[cfg(feature = "messages_enums")]
impl RequestKind {
    /// Encode the message into the target buffer
    #[cfg(feature = "client")]
    pub fn encode(&self, bytes: &mut bytes::BytesMut, version: i16) -> crate::error::Result<()> {
        match self {
            RequestKind::Produce(x) => encode(x, bytes, version),
            RequestKind::Fetch(x) => encode(x, bytes, version),
            RequestKind::ListOffsets(x) => encode(x, bytes, version),
            RequestKind::Metadata(x) => encode(x, bytes, version),
            RequestKind::OffsetCommit(x) => encode(x, bytes, version),
            RequestKind::OffsetFetch(x) => encode(x, bytes, version),
            RequestKind::FindCoordinator(x) => encode(x, bytes, version),
            RequestKind::JoinGroup(x) => encode(x, bytes, version),
            RequestKind::Heartbeat(x) => encode(x, bytes, version),
            RequestKind::LeaveGroup(x) => encode(x, bytes, version),
            RequestKind::SyncGroup(x) => encode(x, bytes, version),
            RequestKind::DescribeGroups(x) => encode(x, bytes, version),
            RequestKind::ListGroups(x) => encode(x, bytes, version),
            RequestKind::SaslHandshake(x) => encode(x, bytes, version),
            RequestKind::ApiVersions(x) => encode(x, bytes, version),
            RequestKind::CreateTopics(x) => encode(x, bytes, version),
            RequestKind::DeleteTopics(x) => encode(x, bytes, version),
            RequestKind::DeleteRecords(x) => encode(x, bytes, version),
            RequestKind::InitProducerId(x) => encode(x, bytes, version),
            RequestKind::OffsetForLeaderEpoch(x) => encode(x, bytes, version),
            RequestKind::AddPartitionsToTxn(x) => encode(x, bytes, version),
            RequestKind::AddOffsetsToTxn(x) => encode(x, bytes, version),
            RequestKind::EndTxn(x) => encode(x, bytes, version),
            RequestKind::WriteTxnMarkers(x) => encode(x, bytes, version),
            RequestKind::TxnOffsetCommit(x) => encode(x, bytes, version),
            RequestKind::DescribeAcls(x) => encode(x, bytes, version),
            RequestKind::CreateAcls(x) => encode(x, bytes, version),
            RequestKind::DeleteAcls(x) => encode(x, bytes, version),
            RequestKind::DescribeConfigs(x) => encode(x, bytes, version),
            RequestKind::AlterConfigs(x) => encode(x, bytes, version),
            RequestKind::AlterReplicaLogDirs(x) => encode(x, bytes, version),
            RequestKind::DescribeLogDirs(x) => encode(x, bytes, version),
            RequestKind::SaslAuthenticate(x) => encode(x, bytes, version),
            RequestKind::CreatePartitions(x) => encode(x, bytes, version),
            RequestKind::CreateDelegationToken(x) => encode(x, bytes, version),
            RequestKind::RenewDelegationToken(x) => encode(x, bytes, version),
            RequestKind::ExpireDelegationToken(x) => encode(x, bytes, version),
            RequestKind::DescribeDelegationToken(x) => encode(x, bytes, version),
            RequestKind::DeleteGroups(x) => encode(x, bytes, version),
            RequestKind::ElectLeaders(x) => encode(x, bytes, version),
            RequestKind::IncrementalAlterConfigs(x) => encode(x, bytes, version),
            RequestKind::AlterPartitionReassignments(x) => encode(x, bytes, version),
            RequestKind::ListPartitionReassignments(x) => encode(x, bytes, version),
            RequestKind::OffsetDelete(x) => encode(x, bytes, version),
            RequestKind::DescribeClientQuotas(x) => encode(x, bytes, version),
            RequestKind::AlterClientQuotas(x) => encode(x, bytes, version),
            RequestKind::DescribeUserScramCredentials(x) => encode(x, bytes, version),
            RequestKind::AlterUserScramCredentials(x) => encode(x, bytes, version),
            RequestKind::Vote(x) => encode(x, bytes, version),
            RequestKind::BeginQuorumEpoch(x) => encode(x, bytes, version),
            RequestKind::EndQuorumEpoch(x) => encode(x, bytes, version),
            RequestKind::DescribeQuorum(x) => encode(x, bytes, version),
            RequestKind::AlterPartition(x) => encode(x, bytes, version),
            RequestKind::UpdateFeatures(x) => encode(x, bytes, version),
            RequestKind::Envelope(x) => encode(x, bytes, version),
            RequestKind::FetchSnapshot(x) => encode(x, bytes, version),
            RequestKind::DescribeCluster(x) => encode(x, bytes, version),
            RequestKind::DescribeProducers(x) => encode(x, bytes, version),
            RequestKind::BrokerRegistration(x) => encode(x, bytes, version),
            RequestKind::BrokerHeartbeat(x) => encode(x, bytes, version),
            RequestKind::UnregisterBroker(x) => encode(x, bytes, version),
            RequestKind::DescribeTransactions(x) => encode(x, bytes, version),
            RequestKind::ListTransactions(x) => encode(x, bytes, version),
            RequestKind::AllocateProducerIds(x) => encode(x, bytes, version),
            RequestKind::ConsumerGroupHeartbeat(x) => encode(x, bytes, version),
            RequestKind::ConsumerGroupDescribe(x) => encode(x, bytes, version),
            RequestKind::ControllerRegistration(x) => encode(x, bytes, version),
            RequestKind::GetTelemetrySubscriptions(x) => encode(x, bytes, version),
            RequestKind::PushTelemetry(x) => encode(x, bytes, version),
            RequestKind::AssignReplicasToDirs(x) => encode(x, bytes, version),
            RequestKind::ListConfigResources(x) => encode(x, bytes, version),
            RequestKind::DescribeTopicPartitions(x) => encode(x, bytes, version),
            RequestKind::ShareGroupHeartbeat(x) => encode(x, bytes, version),
            RequestKind::ShareGroupDescribe(x) => encode(x, bytes, version),
            RequestKind::ShareFetch(x) => encode(x, bytes, version),
            RequestKind::ShareAcknowledge(x) => encode(x, bytes, version),
            RequestKind::AddRaftVoter(x) => encode(x, bytes, version),
            RequestKind::RemoveRaftVoter(x) => encode(x, bytes, version),
            RequestKind::UpdateRaftVoter(x) => encode(x, bytes, version),
            RequestKind::InitializeShareGroupState(x) => encode(x, bytes, version),
            RequestKind::ReadShareGroupState(x) => encode(x, bytes, version),
            RequestKind::WriteShareGroupState(x) => encode(x, bytes, version),
            RequestKind::DeleteShareGroupState(x) => encode(x, bytes, version),
            RequestKind::ReadShareGroupStateSummary(x) => encode(x, bytes, version),
            RequestKind::DescribeShareGroupOffsets(x) => encode(x, bytes, version),
            RequestKind::AlterShareGroupOffsets(x) => encode(x, bytes, version),
            RequestKind::DeleteShareGroupOffsets(x) => encode(x, bytes, version),
        }
    }
    /// Decode the message from the provided buffer and version
    #[cfg(feature = "broker")]
    pub fn decode(
        api_key: ApiKey,
        bytes: &mut bytes::Bytes,
        version: i16,
    ) -> crate::error::Result<RequestKind> {
        match api_key {
            ApiKey::Produce => Ok(RequestKind::Produce(decode(bytes, version)?)),
            ApiKey::Fetch => Ok(RequestKind::Fetch(decode(bytes, version)?)),
            ApiKey::ListOffsets => Ok(RequestKind::ListOffsets(decode(bytes, version)?)),
            ApiKey::Metadata => Ok(RequestKind::Metadata(decode(bytes, version)?)),
            ApiKey::OffsetCommit => Ok(RequestKind::OffsetCommit(decode(bytes, version)?)),
            ApiKey::OffsetFetch => Ok(RequestKind::OffsetFetch(decode(bytes, version)?)),
            ApiKey::FindCoordinator => Ok(RequestKind::FindCoordinator(decode(bytes, version)?)),
            ApiKey::JoinGroup => Ok(RequestKind::JoinGroup(decode(bytes, version)?)),
            ApiKey::Heartbeat => Ok(RequestKind::Heartbeat(decode(bytes, version)?)),
            ApiKey::LeaveGroup => Ok(RequestKind::LeaveGroup(decode(bytes, version)?)),
            ApiKey::SyncGroup => Ok(RequestKind::SyncGroup(decode(bytes, version)?)),
            ApiKey::DescribeGroups => Ok(RequestKind::DescribeGroups(decode(bytes, version)?)),
            ApiKey::ListGroups => Ok(RequestKind::ListGroups(decode(bytes, version)?)),
            ApiKey::SaslHandshake => Ok(RequestKind::SaslHandshake(decode(bytes, version)?)),
            ApiKey::ApiVersions => Ok(RequestKind::ApiVersions(decode(bytes, version)?)),
            ApiKey::CreateTopics => Ok(RequestKind::CreateTopics(decode(bytes, version)?)),
            ApiKey::DeleteTopics => Ok(RequestKind::DeleteTopics(decode(bytes, version)?)),
            ApiKey::DeleteRecords => Ok(RequestKind::DeleteRecords(decode(bytes, version)?)),
            ApiKey::InitProducerId => Ok(RequestKind::InitProducerId(decode(bytes, version)?)),
            ApiKey::OffsetForLeaderEpoch => {
                Ok(RequestKind::OffsetForLeaderEpoch(decode(bytes, version)?))
            }
            ApiKey::AddPartitionsToTxn => {
                Ok(RequestKind::AddPartitionsToTxn(decode(bytes, version)?))
            }
            ApiKey::AddOffsetsToTxn => Ok(RequestKind::AddOffsetsToTxn(decode(bytes, version)?)),
            ApiKey::EndTxn => Ok(RequestKind::EndTxn(decode(bytes, version)?)),
            ApiKey::WriteTxnMarkers => Ok(RequestKind::WriteTxnMarkers(decode(bytes, version)?)),
            ApiKey::TxnOffsetCommit => Ok(RequestKind::TxnOffsetCommit(decode(bytes, version)?)),
            ApiKey::DescribeAcls => Ok(RequestKind::DescribeAcls(decode(bytes, version)?)),
            ApiKey::CreateAcls => Ok(RequestKind::CreateAcls(decode(bytes, version)?)),
            ApiKey::DeleteAcls => Ok(RequestKind::DeleteAcls(decode(bytes, version)?)),
            ApiKey::DescribeConfigs => Ok(RequestKind::DescribeConfigs(decode(bytes, version)?)),
            ApiKey::AlterConfigs => Ok(RequestKind::AlterConfigs(decode(bytes, version)?)),
            ApiKey::AlterReplicaLogDirs => {
                Ok(RequestKind::AlterReplicaLogDirs(decode(bytes, version)?))
            }
            ApiKey::DescribeLogDirs => Ok(RequestKind::DescribeLogDirs(decode(bytes, version)?)),
            ApiKey::SaslAuthenticate => Ok(RequestKind::SaslAuthenticate(decode(bytes, version)?)),
            ApiKey::CreatePartitions => Ok(RequestKind::CreatePartitions(decode(bytes, version)?)),
            ApiKey::CreateDelegationToken => {
                Ok(RequestKind::CreateDelegationToken(decode(bytes, version)?))
            }
            ApiKey::RenewDelegationToken => {
                Ok(RequestKind::RenewDelegationToken(decode(bytes, version)?))
            }
            ApiKey::ExpireDelegationToken => {
                Ok(RequestKind::ExpireDelegationToken(decode(bytes, version)?))
            }
            ApiKey::DescribeDelegationToken => Ok(RequestKind::DescribeDelegationToken(decode(
                bytes, version,
            )?)),
            ApiKey::DeleteGroups => Ok(RequestKind::DeleteGroups(decode(bytes, version)?)),
            ApiKey::ElectLeaders => Ok(RequestKind::ElectLeaders(decode(bytes, version)?)),
            ApiKey::IncrementalAlterConfigs => Ok(RequestKind::IncrementalAlterConfigs(decode(
                bytes, version,
            )?)),
            ApiKey::AlterPartitionReassignments => Ok(RequestKind::AlterPartitionReassignments(
                decode(bytes, version)?,
            )),
            ApiKey::ListPartitionReassignments => Ok(RequestKind::ListPartitionReassignments(
                decode(bytes, version)?,
            )),
            ApiKey::OffsetDelete => Ok(RequestKind::OffsetDelete(decode(bytes, version)?)),
            ApiKey::DescribeClientQuotas => {
                Ok(RequestKind::DescribeClientQuotas(decode(bytes, version)?))
            }
            ApiKey::AlterClientQuotas => {
                Ok(RequestKind::AlterClientQuotas(decode(bytes, version)?))
            }
            ApiKey::DescribeUserScramCredentials => Ok(RequestKind::DescribeUserScramCredentials(
                decode(bytes, version)?,
            )),
            ApiKey::AlterUserScramCredentials => Ok(RequestKind::AlterUserScramCredentials(
                decode(bytes, version)?,
            )),
            ApiKey::Vote => Ok(RequestKind::Vote(decode(bytes, version)?)),
            ApiKey::BeginQuorumEpoch => Ok(RequestKind::BeginQuorumEpoch(decode(bytes, version)?)),
            ApiKey::EndQuorumEpoch => Ok(RequestKind::EndQuorumEpoch(decode(bytes, version)?)),
            ApiKey::DescribeQuorum => Ok(RequestKind::DescribeQuorum(decode(bytes, version)?)),
            ApiKey::AlterPartition => Ok(RequestKind::AlterPartition(decode(bytes, version)?)),
            ApiKey::UpdateFeatures => Ok(RequestKind::UpdateFeatures(decode(bytes, version)?)),
            ApiKey::Envelope => Ok(RequestKind::Envelope(decode(bytes, version)?)),
            ApiKey::FetchSnapshot => Ok(RequestKind::FetchSnapshot(decode(bytes, version)?)),
            ApiKey::DescribeCluster => Ok(RequestKind::DescribeCluster(decode(bytes, version)?)),
            ApiKey::DescribeProducers => {
                Ok(RequestKind::DescribeProducers(decode(bytes, version)?))
            }
            ApiKey::BrokerRegistration => {
                Ok(RequestKind::BrokerRegistration(decode(bytes, version)?))
            }
            ApiKey::BrokerHeartbeat => Ok(RequestKind::BrokerHeartbeat(decode(bytes, version)?)),
            ApiKey::UnregisterBroker => Ok(RequestKind::UnregisterBroker(decode(bytes, version)?)),
            ApiKey::DescribeTransactions => {
                Ok(RequestKind::DescribeTransactions(decode(bytes, version)?))
            }
            ApiKey::ListTransactions => Ok(RequestKind::ListTransactions(decode(bytes, version)?)),
            ApiKey::AllocateProducerIds => {
                Ok(RequestKind::AllocateProducerIds(decode(bytes, version)?))
            }
            ApiKey::ConsumerGroupHeartbeat => {
                Ok(RequestKind::ConsumerGroupHeartbeat(decode(bytes, version)?))
            }
            ApiKey::ConsumerGroupDescribe => {
                Ok(RequestKind::ConsumerGroupDescribe(decode(bytes, version)?))
            }
            ApiKey::ControllerRegistration => {
                Ok(RequestKind::ControllerRegistration(decode(bytes, version)?))
            }
            ApiKey::GetTelemetrySubscriptions => Ok(RequestKind::GetTelemetrySubscriptions(
                decode(bytes, version)?,
            )),
            ApiKey::PushTelemetry => Ok(RequestKind::PushTelemetry(decode(bytes, version)?)),
            ApiKey::AssignReplicasToDirs => {
                Ok(RequestKind::AssignReplicasToDirs(decode(bytes, version)?))
            }
            ApiKey::ListConfigResources => {
                Ok(RequestKind::ListConfigResources(decode(bytes, version)?))
            }
            ApiKey::DescribeTopicPartitions => Ok(RequestKind::DescribeTopicPartitions(decode(
                bytes, version,
            )?)),
            ApiKey::ShareGroupHeartbeat => {
                Ok(RequestKind::ShareGroupHeartbeat(decode(bytes, version)?))
            }
            ApiKey::ShareGroupDescribe => {
                Ok(RequestKind::ShareGroupDescribe(decode(bytes, version)?))
            }
            ApiKey::ShareFetch => Ok(RequestKind::ShareFetch(decode(bytes, version)?)),
            ApiKey::ShareAcknowledge => Ok(RequestKind::ShareAcknowledge(decode(bytes, version)?)),
            ApiKey::AddRaftVoter => Ok(RequestKind::AddRaftVoter(decode(bytes, version)?)),
            ApiKey::RemoveRaftVoter => Ok(RequestKind::RemoveRaftVoter(decode(bytes, version)?)),
            ApiKey::UpdateRaftVoter => Ok(RequestKind::UpdateRaftVoter(decode(bytes, version)?)),
            ApiKey::InitializeShareGroupState => Ok(RequestKind::InitializeShareGroupState(
                decode(bytes, version)?,
            )),
            ApiKey::ReadShareGroupState => {
                Ok(RequestKind::ReadShareGroupState(decode(bytes, version)?))
            }
            ApiKey::WriteShareGroupState => {
                Ok(RequestKind::WriteShareGroupState(decode(bytes, version)?))
            }
            ApiKey::DeleteShareGroupState => {
                Ok(RequestKind::DeleteShareGroupState(decode(bytes, version)?))
            }
            ApiKey::ReadShareGroupStateSummary => Ok(RequestKind::ReadShareGroupStateSummary(
                decode(bytes, version)?,
            )),
            ApiKey::DescribeShareGroupOffsets => Ok(RequestKind::DescribeShareGroupOffsets(
                decode(bytes, version)?,
            )),
            ApiKey::AlterShareGroupOffsets => {
                Ok(RequestKind::AlterShareGroupOffsets(decode(bytes, version)?))
            }
            ApiKey::DeleteShareGroupOffsets => Ok(RequestKind::DeleteShareGroupOffsets(decode(
                bytes, version,
            )?)),
        }
    }
}
#[cfg(feature = "messages_enums")]
impl From<ProduceRequest> for RequestKind {
    fn from(value: ProduceRequest) -> RequestKind {
        RequestKind::Produce(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FetchRequest> for RequestKind {
    fn from(value: FetchRequest) -> RequestKind {
        RequestKind::Fetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListOffsetsRequest> for RequestKind {
    fn from(value: ListOffsetsRequest) -> RequestKind {
        RequestKind::ListOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<MetadataRequest> for RequestKind {
    fn from(value: MetadataRequest) -> RequestKind {
        RequestKind::Metadata(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetCommitRequest> for RequestKind {
    fn from(value: OffsetCommitRequest) -> RequestKind {
        RequestKind::OffsetCommit(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetFetchRequest> for RequestKind {
    fn from(value: OffsetFetchRequest) -> RequestKind {
        RequestKind::OffsetFetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FindCoordinatorRequest> for RequestKind {
    fn from(value: FindCoordinatorRequest) -> RequestKind {
        RequestKind::FindCoordinator(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<JoinGroupRequest> for RequestKind {
    fn from(value: JoinGroupRequest) -> RequestKind {
        RequestKind::JoinGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<HeartbeatRequest> for RequestKind {
    fn from(value: HeartbeatRequest) -> RequestKind {
        RequestKind::Heartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<LeaveGroupRequest> for RequestKind {
    fn from(value: LeaveGroupRequest) -> RequestKind {
        RequestKind::LeaveGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SyncGroupRequest> for RequestKind {
    fn from(value: SyncGroupRequest) -> RequestKind {
        RequestKind::SyncGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeGroupsRequest> for RequestKind {
    fn from(value: DescribeGroupsRequest) -> RequestKind {
        RequestKind::DescribeGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListGroupsRequest> for RequestKind {
    fn from(value: ListGroupsRequest) -> RequestKind {
        RequestKind::ListGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SaslHandshakeRequest> for RequestKind {
    fn from(value: SaslHandshakeRequest) -> RequestKind {
        RequestKind::SaslHandshake(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ApiVersionsRequest> for RequestKind {
    fn from(value: ApiVersionsRequest) -> RequestKind {
        RequestKind::ApiVersions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateTopicsRequest> for RequestKind {
    fn from(value: CreateTopicsRequest) -> RequestKind {
        RequestKind::CreateTopics(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteTopicsRequest> for RequestKind {
    fn from(value: DeleteTopicsRequest) -> RequestKind {
        RequestKind::DeleteTopics(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteRecordsRequest> for RequestKind {
    fn from(value: DeleteRecordsRequest) -> RequestKind {
        RequestKind::DeleteRecords(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<InitProducerIdRequest> for RequestKind {
    fn from(value: InitProducerIdRequest) -> RequestKind {
        RequestKind::InitProducerId(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetForLeaderEpochRequest> for RequestKind {
    fn from(value: OffsetForLeaderEpochRequest) -> RequestKind {
        RequestKind::OffsetForLeaderEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddPartitionsToTxnRequest> for RequestKind {
    fn from(value: AddPartitionsToTxnRequest) -> RequestKind {
        RequestKind::AddPartitionsToTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddOffsetsToTxnRequest> for RequestKind {
    fn from(value: AddOffsetsToTxnRequest) -> RequestKind {
        RequestKind::AddOffsetsToTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EndTxnRequest> for RequestKind {
    fn from(value: EndTxnRequest) -> RequestKind {
        RequestKind::EndTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<WriteTxnMarkersRequest> for RequestKind {
    fn from(value: WriteTxnMarkersRequest) -> RequestKind {
        RequestKind::WriteTxnMarkers(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<TxnOffsetCommitRequest> for RequestKind {
    fn from(value: TxnOffsetCommitRequest) -> RequestKind {
        RequestKind::TxnOffsetCommit(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeAclsRequest> for RequestKind {
    fn from(value: DescribeAclsRequest) -> RequestKind {
        RequestKind::DescribeAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateAclsRequest> for RequestKind {
    fn from(value: CreateAclsRequest) -> RequestKind {
        RequestKind::CreateAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteAclsRequest> for RequestKind {
    fn from(value: DeleteAclsRequest) -> RequestKind {
        RequestKind::DeleteAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeConfigsRequest> for RequestKind {
    fn from(value: DescribeConfigsRequest) -> RequestKind {
        RequestKind::DescribeConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterConfigsRequest> for RequestKind {
    fn from(value: AlterConfigsRequest) -> RequestKind {
        RequestKind::AlterConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterReplicaLogDirsRequest> for RequestKind {
    fn from(value: AlterReplicaLogDirsRequest) -> RequestKind {
        RequestKind::AlterReplicaLogDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeLogDirsRequest> for RequestKind {
    fn from(value: DescribeLogDirsRequest) -> RequestKind {
        RequestKind::DescribeLogDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SaslAuthenticateRequest> for RequestKind {
    fn from(value: SaslAuthenticateRequest) -> RequestKind {
        RequestKind::SaslAuthenticate(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreatePartitionsRequest> for RequestKind {
    fn from(value: CreatePartitionsRequest) -> RequestKind {
        RequestKind::CreatePartitions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateDelegationTokenRequest> for RequestKind {
    fn from(value: CreateDelegationTokenRequest) -> RequestKind {
        RequestKind::CreateDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<RenewDelegationTokenRequest> for RequestKind {
    fn from(value: RenewDelegationTokenRequest) -> RequestKind {
        RequestKind::RenewDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ExpireDelegationTokenRequest> for RequestKind {
    fn from(value: ExpireDelegationTokenRequest) -> RequestKind {
        RequestKind::ExpireDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeDelegationTokenRequest> for RequestKind {
    fn from(value: DescribeDelegationTokenRequest) -> RequestKind {
        RequestKind::DescribeDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteGroupsRequest> for RequestKind {
    fn from(value: DeleteGroupsRequest) -> RequestKind {
        RequestKind::DeleteGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ElectLeadersRequest> for RequestKind {
    fn from(value: ElectLeadersRequest) -> RequestKind {
        RequestKind::ElectLeaders(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<IncrementalAlterConfigsRequest> for RequestKind {
    fn from(value: IncrementalAlterConfigsRequest) -> RequestKind {
        RequestKind::IncrementalAlterConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterPartitionReassignmentsRequest> for RequestKind {
    fn from(value: AlterPartitionReassignmentsRequest) -> RequestKind {
        RequestKind::AlterPartitionReassignments(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListPartitionReassignmentsRequest> for RequestKind {
    fn from(value: ListPartitionReassignmentsRequest) -> RequestKind {
        RequestKind::ListPartitionReassignments(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetDeleteRequest> for RequestKind {
    fn from(value: OffsetDeleteRequest) -> RequestKind {
        RequestKind::OffsetDelete(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeClientQuotasRequest> for RequestKind {
    fn from(value: DescribeClientQuotasRequest) -> RequestKind {
        RequestKind::DescribeClientQuotas(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterClientQuotasRequest> for RequestKind {
    fn from(value: AlterClientQuotasRequest) -> RequestKind {
        RequestKind::AlterClientQuotas(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeUserScramCredentialsRequest> for RequestKind {
    fn from(value: DescribeUserScramCredentialsRequest) -> RequestKind {
        RequestKind::DescribeUserScramCredentials(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterUserScramCredentialsRequest> for RequestKind {
    fn from(value: AlterUserScramCredentialsRequest) -> RequestKind {
        RequestKind::AlterUserScramCredentials(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<VoteRequest> for RequestKind {
    fn from(value: VoteRequest) -> RequestKind {
        RequestKind::Vote(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BeginQuorumEpochRequest> for RequestKind {
    fn from(value: BeginQuorumEpochRequest) -> RequestKind {
        RequestKind::BeginQuorumEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EndQuorumEpochRequest> for RequestKind {
    fn from(value: EndQuorumEpochRequest) -> RequestKind {
        RequestKind::EndQuorumEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeQuorumRequest> for RequestKind {
    fn from(value: DescribeQuorumRequest) -> RequestKind {
        RequestKind::DescribeQuorum(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterPartitionRequest> for RequestKind {
    fn from(value: AlterPartitionRequest) -> RequestKind {
        RequestKind::AlterPartition(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UpdateFeaturesRequest> for RequestKind {
    fn from(value: UpdateFeaturesRequest) -> RequestKind {
        RequestKind::UpdateFeatures(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EnvelopeRequest> for RequestKind {
    fn from(value: EnvelopeRequest) -> RequestKind {
        RequestKind::Envelope(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FetchSnapshotRequest> for RequestKind {
    fn from(value: FetchSnapshotRequest) -> RequestKind {
        RequestKind::FetchSnapshot(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeClusterRequest> for RequestKind {
    fn from(value: DescribeClusterRequest) -> RequestKind {
        RequestKind::DescribeCluster(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeProducersRequest> for RequestKind {
    fn from(value: DescribeProducersRequest) -> RequestKind {
        RequestKind::DescribeProducers(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BrokerRegistrationRequest> for RequestKind {
    fn from(value: BrokerRegistrationRequest) -> RequestKind {
        RequestKind::BrokerRegistration(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BrokerHeartbeatRequest> for RequestKind {
    fn from(value: BrokerHeartbeatRequest) -> RequestKind {
        RequestKind::BrokerHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UnregisterBrokerRequest> for RequestKind {
    fn from(value: UnregisterBrokerRequest) -> RequestKind {
        RequestKind::UnregisterBroker(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeTransactionsRequest> for RequestKind {
    fn from(value: DescribeTransactionsRequest) -> RequestKind {
        RequestKind::DescribeTransactions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListTransactionsRequest> for RequestKind {
    fn from(value: ListTransactionsRequest) -> RequestKind {
        RequestKind::ListTransactions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AllocateProducerIdsRequest> for RequestKind {
    fn from(value: AllocateProducerIdsRequest) -> RequestKind {
        RequestKind::AllocateProducerIds(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ConsumerGroupHeartbeatRequest> for RequestKind {
    fn from(value: ConsumerGroupHeartbeatRequest) -> RequestKind {
        RequestKind::ConsumerGroupHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ConsumerGroupDescribeRequest> for RequestKind {
    fn from(value: ConsumerGroupDescribeRequest) -> RequestKind {
        RequestKind::ConsumerGroupDescribe(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ControllerRegistrationRequest> for RequestKind {
    fn from(value: ControllerRegistrationRequest) -> RequestKind {
        RequestKind::ControllerRegistration(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<GetTelemetrySubscriptionsRequest> for RequestKind {
    fn from(value: GetTelemetrySubscriptionsRequest) -> RequestKind {
        RequestKind::GetTelemetrySubscriptions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<PushTelemetryRequest> for RequestKind {
    fn from(value: PushTelemetryRequest) -> RequestKind {
        RequestKind::PushTelemetry(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AssignReplicasToDirsRequest> for RequestKind {
    fn from(value: AssignReplicasToDirsRequest) -> RequestKind {
        RequestKind::AssignReplicasToDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListConfigResourcesRequest> for RequestKind {
    fn from(value: ListConfigResourcesRequest) -> RequestKind {
        RequestKind::ListConfigResources(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeTopicPartitionsRequest> for RequestKind {
    fn from(value: DescribeTopicPartitionsRequest) -> RequestKind {
        RequestKind::DescribeTopicPartitions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareGroupHeartbeatRequest> for RequestKind {
    fn from(value: ShareGroupHeartbeatRequest) -> RequestKind {
        RequestKind::ShareGroupHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareGroupDescribeRequest> for RequestKind {
    fn from(value: ShareGroupDescribeRequest) -> RequestKind {
        RequestKind::ShareGroupDescribe(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareFetchRequest> for RequestKind {
    fn from(value: ShareFetchRequest) -> RequestKind {
        RequestKind::ShareFetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareAcknowledgeRequest> for RequestKind {
    fn from(value: ShareAcknowledgeRequest) -> RequestKind {
        RequestKind::ShareAcknowledge(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddRaftVoterRequest> for RequestKind {
    fn from(value: AddRaftVoterRequest) -> RequestKind {
        RequestKind::AddRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<RemoveRaftVoterRequest> for RequestKind {
    fn from(value: RemoveRaftVoterRequest) -> RequestKind {
        RequestKind::RemoveRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UpdateRaftVoterRequest> for RequestKind {
    fn from(value: UpdateRaftVoterRequest) -> RequestKind {
        RequestKind::UpdateRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<InitializeShareGroupStateRequest> for RequestKind {
    fn from(value: InitializeShareGroupStateRequest) -> RequestKind {
        RequestKind::InitializeShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ReadShareGroupStateRequest> for RequestKind {
    fn from(value: ReadShareGroupStateRequest) -> RequestKind {
        RequestKind::ReadShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<WriteShareGroupStateRequest> for RequestKind {
    fn from(value: WriteShareGroupStateRequest) -> RequestKind {
        RequestKind::WriteShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteShareGroupStateRequest> for RequestKind {
    fn from(value: DeleteShareGroupStateRequest) -> RequestKind {
        RequestKind::DeleteShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ReadShareGroupStateSummaryRequest> for RequestKind {
    fn from(value: ReadShareGroupStateSummaryRequest) -> RequestKind {
        RequestKind::ReadShareGroupStateSummary(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeShareGroupOffsetsRequest> for RequestKind {
    fn from(value: DescribeShareGroupOffsetsRequest) -> RequestKind {
        RequestKind::DescribeShareGroupOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterShareGroupOffsetsRequest> for RequestKind {
    fn from(value: AlterShareGroupOffsetsRequest) -> RequestKind {
        RequestKind::AlterShareGroupOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteShareGroupOffsetsRequest> for RequestKind {
    fn from(value: DeleteShareGroupOffsetsRequest) -> RequestKind {
        RequestKind::DeleteShareGroupOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
fn decode<T: Decodable>(bytes: &mut bytes::Bytes, version: i16) -> Result<T> {
    T::decode(bytes, version).map_err(|e| {
        crate::error::ProtocolError::Other(format!(
            "Failed to decode {} v{} body: {e}",
            std::any::type_name::<T>(),
            version
        ))
    })
}

#[cfg(feature = "messages_enums")]
#[cfg(any(feature = "client", feature = "broker"))]
fn encode<T: Encodable>(encodable: &T, bytes: &mut bytes::BytesMut, version: i16) -> Result<()> {
    encodable.encode(bytes, version).map_err(|e| {
        crate::error::ProtocolError::Other(format!(
            "Failed to encode {} v{} body: {e}",
            std::any::type_name::<T>(),
            version
        ))
    })
}

#[cfg(feature = "messages_enums")]
#[cfg(feature = "broker")]
fn encode_into<T: Encodable, B: crate::protocol::buf::ByteBufMut>(
    encodable: &T,
    buf: &mut B,
    version: i16,
) -> Result<()> {
    encodable.encode(buf, version).map_err(|e| {
        crate::error::ProtocolError::Other(format!(
            "Failed to encode {} v{} body: {e}",
            std::any::type_name::<T>(),
            version
        ))
    })
}

/// Wrapping enum for all responses in the Kafka protocol.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
#[cfg(feature = "messages_enums")]
pub enum ResponseKind {
    /// ProduceResponse,
    Produce(ProduceResponse),
    /// FetchResponse,
    Fetch(FetchResponse),
    /// ListOffsetsResponse,
    ListOffsets(ListOffsetsResponse),
    /// MetadataResponse,
    Metadata(MetadataResponse),
    /// OffsetCommitResponse,
    OffsetCommit(OffsetCommitResponse),
    /// OffsetFetchResponse,
    OffsetFetch(OffsetFetchResponse),
    /// FindCoordinatorResponse,
    FindCoordinator(FindCoordinatorResponse),
    /// JoinGroupResponse,
    JoinGroup(JoinGroupResponse),
    /// HeartbeatResponse,
    Heartbeat(HeartbeatResponse),
    /// LeaveGroupResponse,
    LeaveGroup(LeaveGroupResponse),
    /// SyncGroupResponse,
    SyncGroup(SyncGroupResponse),
    /// DescribeGroupsResponse,
    DescribeGroups(DescribeGroupsResponse),
    /// ListGroupsResponse,
    ListGroups(ListGroupsResponse),
    /// SaslHandshakeResponse,
    SaslHandshake(SaslHandshakeResponse),
    /// ApiVersionsResponse,
    ApiVersions(ApiVersionsResponse),
    /// CreateTopicsResponse,
    CreateTopics(CreateTopicsResponse),
    /// DeleteTopicsResponse,
    DeleteTopics(DeleteTopicsResponse),
    /// DeleteRecordsResponse,
    DeleteRecords(DeleteRecordsResponse),
    /// InitProducerIdResponse,
    InitProducerId(InitProducerIdResponse),
    /// OffsetForLeaderEpochResponse,
    OffsetForLeaderEpoch(OffsetForLeaderEpochResponse),
    /// AddPartitionsToTxnResponse,
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    /// AddOffsetsToTxnResponse,
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    /// EndTxnResponse,
    EndTxn(EndTxnResponse),
    /// WriteTxnMarkersResponse,
    WriteTxnMarkers(WriteTxnMarkersResponse),
    /// TxnOffsetCommitResponse,
    TxnOffsetCommit(TxnOffsetCommitResponse),
    /// DescribeAclsResponse,
    DescribeAcls(DescribeAclsResponse),
    /// CreateAclsResponse,
    CreateAcls(CreateAclsResponse),
    /// DeleteAclsResponse,
    DeleteAcls(DeleteAclsResponse),
    /// DescribeConfigsResponse,
    DescribeConfigs(DescribeConfigsResponse),
    /// AlterConfigsResponse,
    AlterConfigs(AlterConfigsResponse),
    /// AlterReplicaLogDirsResponse,
    AlterReplicaLogDirs(AlterReplicaLogDirsResponse),
    /// DescribeLogDirsResponse,
    DescribeLogDirs(DescribeLogDirsResponse),
    /// SaslAuthenticateResponse,
    SaslAuthenticate(SaslAuthenticateResponse),
    /// CreatePartitionsResponse,
    CreatePartitions(CreatePartitionsResponse),
    /// CreateDelegationTokenResponse,
    CreateDelegationToken(CreateDelegationTokenResponse),
    /// RenewDelegationTokenResponse,
    RenewDelegationToken(RenewDelegationTokenResponse),
    /// ExpireDelegationTokenResponse,
    ExpireDelegationToken(ExpireDelegationTokenResponse),
    /// DescribeDelegationTokenResponse,
    DescribeDelegationToken(DescribeDelegationTokenResponse),
    /// DeleteGroupsResponse,
    DeleteGroups(DeleteGroupsResponse),
    /// ElectLeadersResponse,
    ElectLeaders(ElectLeadersResponse),
    /// IncrementalAlterConfigsResponse,
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    /// AlterPartitionReassignmentsResponse,
    AlterPartitionReassignments(AlterPartitionReassignmentsResponse),
    /// ListPartitionReassignmentsResponse,
    ListPartitionReassignments(ListPartitionReassignmentsResponse),
    /// OffsetDeleteResponse,
    OffsetDelete(OffsetDeleteResponse),
    /// DescribeClientQuotasResponse,
    DescribeClientQuotas(DescribeClientQuotasResponse),
    /// AlterClientQuotasResponse,
    AlterClientQuotas(AlterClientQuotasResponse),
    /// DescribeUserScramCredentialsResponse,
    DescribeUserScramCredentials(DescribeUserScramCredentialsResponse),
    /// AlterUserScramCredentialsResponse,
    AlterUserScramCredentials(AlterUserScramCredentialsResponse),
    /// VoteResponse,
    Vote(VoteResponse),
    /// BeginQuorumEpochResponse,
    BeginQuorumEpoch(BeginQuorumEpochResponse),
    /// EndQuorumEpochResponse,
    EndQuorumEpoch(EndQuorumEpochResponse),
    /// DescribeQuorumResponse,
    DescribeQuorum(DescribeQuorumResponse),
    /// AlterPartitionResponse,
    AlterPartition(AlterPartitionResponse),
    /// UpdateFeaturesResponse,
    UpdateFeatures(UpdateFeaturesResponse),
    /// EnvelopeResponse,
    Envelope(EnvelopeResponse),
    /// FetchSnapshotResponse,
    FetchSnapshot(FetchSnapshotResponse),
    /// DescribeClusterResponse,
    DescribeCluster(DescribeClusterResponse),
    /// DescribeProducersResponse,
    DescribeProducers(DescribeProducersResponse),
    /// BrokerRegistrationResponse,
    BrokerRegistration(BrokerRegistrationResponse),
    /// BrokerHeartbeatResponse,
    BrokerHeartbeat(BrokerHeartbeatResponse),
    /// UnregisterBrokerResponse,
    UnregisterBroker(UnregisterBrokerResponse),
    /// DescribeTransactionsResponse,
    DescribeTransactions(DescribeTransactionsResponse),
    /// ListTransactionsResponse,
    ListTransactions(ListTransactionsResponse),
    /// AllocateProducerIdsResponse,
    AllocateProducerIds(AllocateProducerIdsResponse),
    /// ConsumerGroupHeartbeatResponse,
    ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponse),
    /// ConsumerGroupDescribeResponse,
    ConsumerGroupDescribe(ConsumerGroupDescribeResponse),
    /// ControllerRegistrationResponse,
    ControllerRegistration(ControllerRegistrationResponse),
    /// GetTelemetrySubscriptionsResponse,
    GetTelemetrySubscriptions(GetTelemetrySubscriptionsResponse),
    /// PushTelemetryResponse,
    PushTelemetry(PushTelemetryResponse),
    /// AssignReplicasToDirsResponse,
    AssignReplicasToDirs(AssignReplicasToDirsResponse),
    /// ListConfigResourcesResponse,
    ListConfigResources(ListConfigResourcesResponse),
    /// DescribeTopicPartitionsResponse,
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
    /// ShareGroupHeartbeatResponse,
    ShareGroupHeartbeat(ShareGroupHeartbeatResponse),
    /// ShareGroupDescribeResponse,
    ShareGroupDescribe(ShareGroupDescribeResponse),
    /// ShareFetchResponse,
    ShareFetch(ShareFetchResponse),
    /// ShareAcknowledgeResponse,
    ShareAcknowledge(ShareAcknowledgeResponse),
    /// AddRaftVoterResponse,
    AddRaftVoter(AddRaftVoterResponse),
    /// RemoveRaftVoterResponse,
    RemoveRaftVoter(RemoveRaftVoterResponse),
    /// UpdateRaftVoterResponse,
    UpdateRaftVoter(UpdateRaftVoterResponse),
    /// InitializeShareGroupStateResponse,
    InitializeShareGroupState(InitializeShareGroupStateResponse),
    /// ReadShareGroupStateResponse,
    ReadShareGroupState(ReadShareGroupStateResponse),
    /// WriteShareGroupStateResponse,
    WriteShareGroupState(WriteShareGroupStateResponse),
    /// DeleteShareGroupStateResponse,
    DeleteShareGroupState(DeleteShareGroupStateResponse),
    /// ReadShareGroupStateSummaryResponse,
    ReadShareGroupStateSummary(ReadShareGroupStateSummaryResponse),
    /// DescribeShareGroupOffsetsResponse,
    DescribeShareGroupOffsets(DescribeShareGroupOffsetsResponse),
    /// AlterShareGroupOffsetsResponse,
    AlterShareGroupOffsets(AlterShareGroupOffsetsResponse),
    /// DeleteShareGroupOffsetsResponse,
    DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse),
}

#[cfg(feature = "messages_enums")]
impl ResponseKind {
    /// Encode the message into the target buffer
    #[cfg(feature = "broker")]
    pub fn encode(&self, bytes: &mut bytes::BytesMut, version: i16) -> crate::error::Result<()> {
        match self {
            ResponseKind::Produce(x) => encode(x, bytes, version),
            ResponseKind::Fetch(x) => encode(x, bytes, version),
            ResponseKind::ListOffsets(x) => encode(x, bytes, version),
            ResponseKind::Metadata(x) => encode(x, bytes, version),
            ResponseKind::OffsetCommit(x) => encode(x, bytes, version),
            ResponseKind::OffsetFetch(x) => encode(x, bytes, version),
            ResponseKind::FindCoordinator(x) => encode(x, bytes, version),
            ResponseKind::JoinGroup(x) => encode(x, bytes, version),
            ResponseKind::Heartbeat(x) => encode(x, bytes, version),
            ResponseKind::LeaveGroup(x) => encode(x, bytes, version),
            ResponseKind::SyncGroup(x) => encode(x, bytes, version),
            ResponseKind::DescribeGroups(x) => encode(x, bytes, version),
            ResponseKind::ListGroups(x) => encode(x, bytes, version),
            ResponseKind::SaslHandshake(x) => encode(x, bytes, version),
            ResponseKind::ApiVersions(x) => encode(x, bytes, version),
            ResponseKind::CreateTopics(x) => encode(x, bytes, version),
            ResponseKind::DeleteTopics(x) => encode(x, bytes, version),
            ResponseKind::DeleteRecords(x) => encode(x, bytes, version),
            ResponseKind::InitProducerId(x) => encode(x, bytes, version),
            ResponseKind::OffsetForLeaderEpoch(x) => encode(x, bytes, version),
            ResponseKind::AddPartitionsToTxn(x) => encode(x, bytes, version),
            ResponseKind::AddOffsetsToTxn(x) => encode(x, bytes, version),
            ResponseKind::EndTxn(x) => encode(x, bytes, version),
            ResponseKind::WriteTxnMarkers(x) => encode(x, bytes, version),
            ResponseKind::TxnOffsetCommit(x) => encode(x, bytes, version),
            ResponseKind::DescribeAcls(x) => encode(x, bytes, version),
            ResponseKind::CreateAcls(x) => encode(x, bytes, version),
            ResponseKind::DeleteAcls(x) => encode(x, bytes, version),
            ResponseKind::DescribeConfigs(x) => encode(x, bytes, version),
            ResponseKind::AlterConfigs(x) => encode(x, bytes, version),
            ResponseKind::AlterReplicaLogDirs(x) => encode(x, bytes, version),
            ResponseKind::DescribeLogDirs(x) => encode(x, bytes, version),
            ResponseKind::SaslAuthenticate(x) => encode(x, bytes, version),
            ResponseKind::CreatePartitions(x) => encode(x, bytes, version),
            ResponseKind::CreateDelegationToken(x) => encode(x, bytes, version),
            ResponseKind::RenewDelegationToken(x) => encode(x, bytes, version),
            ResponseKind::ExpireDelegationToken(x) => encode(x, bytes, version),
            ResponseKind::DescribeDelegationToken(x) => encode(x, bytes, version),
            ResponseKind::DeleteGroups(x) => encode(x, bytes, version),
            ResponseKind::ElectLeaders(x) => encode(x, bytes, version),
            ResponseKind::IncrementalAlterConfigs(x) => encode(x, bytes, version),
            ResponseKind::AlterPartitionReassignments(x) => encode(x, bytes, version),
            ResponseKind::ListPartitionReassignments(x) => encode(x, bytes, version),
            ResponseKind::OffsetDelete(x) => encode(x, bytes, version),
            ResponseKind::DescribeClientQuotas(x) => encode(x, bytes, version),
            ResponseKind::AlterClientQuotas(x) => encode(x, bytes, version),
            ResponseKind::DescribeUserScramCredentials(x) => encode(x, bytes, version),
            ResponseKind::AlterUserScramCredentials(x) => encode(x, bytes, version),
            ResponseKind::Vote(x) => encode(x, bytes, version),
            ResponseKind::BeginQuorumEpoch(x) => encode(x, bytes, version),
            ResponseKind::EndQuorumEpoch(x) => encode(x, bytes, version),
            ResponseKind::DescribeQuorum(x) => encode(x, bytes, version),
            ResponseKind::AlterPartition(x) => encode(x, bytes, version),
            ResponseKind::UpdateFeatures(x) => encode(x, bytes, version),
            ResponseKind::Envelope(x) => encode(x, bytes, version),
            ResponseKind::FetchSnapshot(x) => encode(x, bytes, version),
            ResponseKind::DescribeCluster(x) => encode(x, bytes, version),
            ResponseKind::DescribeProducers(x) => encode(x, bytes, version),
            ResponseKind::BrokerRegistration(x) => encode(x, bytes, version),
            ResponseKind::BrokerHeartbeat(x) => encode(x, bytes, version),
            ResponseKind::UnregisterBroker(x) => encode(x, bytes, version),
            ResponseKind::DescribeTransactions(x) => encode(x, bytes, version),
            ResponseKind::ListTransactions(x) => encode(x, bytes, version),
            ResponseKind::AllocateProducerIds(x) => encode(x, bytes, version),
            ResponseKind::ConsumerGroupHeartbeat(x) => encode(x, bytes, version),
            ResponseKind::ConsumerGroupDescribe(x) => encode(x, bytes, version),
            ResponseKind::ControllerRegistration(x) => encode(x, bytes, version),
            ResponseKind::GetTelemetrySubscriptions(x) => encode(x, bytes, version),
            ResponseKind::PushTelemetry(x) => encode(x, bytes, version),
            ResponseKind::AssignReplicasToDirs(x) => encode(x, bytes, version),
            ResponseKind::ListConfigResources(x) => encode(x, bytes, version),
            ResponseKind::DescribeTopicPartitions(x) => encode(x, bytes, version),
            ResponseKind::ShareGroupHeartbeat(x) => encode(x, bytes, version),
            ResponseKind::ShareGroupDescribe(x) => encode(x, bytes, version),
            ResponseKind::ShareFetch(x) => encode(x, bytes, version),
            ResponseKind::ShareAcknowledge(x) => encode(x, bytes, version),
            ResponseKind::AddRaftVoter(x) => encode(x, bytes, version),
            ResponseKind::RemoveRaftVoter(x) => encode(x, bytes, version),
            ResponseKind::UpdateRaftVoter(x) => encode(x, bytes, version),
            ResponseKind::InitializeShareGroupState(x) => encode(x, bytes, version),
            ResponseKind::ReadShareGroupState(x) => encode(x, bytes, version),
            ResponseKind::WriteShareGroupState(x) => encode(x, bytes, version),
            ResponseKind::DeleteShareGroupState(x) => encode(x, bytes, version),
            ResponseKind::ReadShareGroupStateSummary(x) => encode(x, bytes, version),
            ResponseKind::DescribeShareGroupOffsets(x) => encode(x, bytes, version),
            ResponseKind::AlterShareGroupOffsets(x) => encode(x, bytes, version),
            ResponseKind::DeleteShareGroupOffsets(x) => encode(x, bytes, version),
        }
    }
    /// Encode the message into a `ByteBufMut` (e.g. `SegmentedBuf` for zero-copy).
    #[cfg(feature = "broker")]
    pub fn encode_into<B: crate::protocol::buf::ByteBufMut>(
        &self,
        buf: &mut B,
        version: i16,
    ) -> crate::error::Result<()> {
        match self {
            ResponseKind::Produce(x) => encode_into(x, buf, version),
            ResponseKind::Fetch(x) => encode_into(x, buf, version),
            ResponseKind::ListOffsets(x) => encode_into(x, buf, version),
            ResponseKind::Metadata(x) => encode_into(x, buf, version),
            ResponseKind::OffsetCommit(x) => encode_into(x, buf, version),
            ResponseKind::OffsetFetch(x) => encode_into(x, buf, version),
            ResponseKind::FindCoordinator(x) => encode_into(x, buf, version),
            ResponseKind::JoinGroup(x) => encode_into(x, buf, version),
            ResponseKind::Heartbeat(x) => encode_into(x, buf, version),
            ResponseKind::LeaveGroup(x) => encode_into(x, buf, version),
            ResponseKind::SyncGroup(x) => encode_into(x, buf, version),
            ResponseKind::DescribeGroups(x) => encode_into(x, buf, version),
            ResponseKind::ListGroups(x) => encode_into(x, buf, version),
            ResponseKind::SaslHandshake(x) => encode_into(x, buf, version),
            ResponseKind::ApiVersions(x) => encode_into(x, buf, version),
            ResponseKind::CreateTopics(x) => encode_into(x, buf, version),
            ResponseKind::DeleteTopics(x) => encode_into(x, buf, version),
            ResponseKind::DeleteRecords(x) => encode_into(x, buf, version),
            ResponseKind::InitProducerId(x) => encode_into(x, buf, version),
            ResponseKind::OffsetForLeaderEpoch(x) => encode_into(x, buf, version),
            ResponseKind::AddPartitionsToTxn(x) => encode_into(x, buf, version),
            ResponseKind::AddOffsetsToTxn(x) => encode_into(x, buf, version),
            ResponseKind::EndTxn(x) => encode_into(x, buf, version),
            ResponseKind::WriteTxnMarkers(x) => encode_into(x, buf, version),
            ResponseKind::TxnOffsetCommit(x) => encode_into(x, buf, version),
            ResponseKind::DescribeAcls(x) => encode_into(x, buf, version),
            ResponseKind::CreateAcls(x) => encode_into(x, buf, version),
            ResponseKind::DeleteAcls(x) => encode_into(x, buf, version),
            ResponseKind::DescribeConfigs(x) => encode_into(x, buf, version),
            ResponseKind::AlterConfigs(x) => encode_into(x, buf, version),
            ResponseKind::AlterReplicaLogDirs(x) => encode_into(x, buf, version),
            ResponseKind::DescribeLogDirs(x) => encode_into(x, buf, version),
            ResponseKind::SaslAuthenticate(x) => encode_into(x, buf, version),
            ResponseKind::CreatePartitions(x) => encode_into(x, buf, version),
            ResponseKind::CreateDelegationToken(x) => encode_into(x, buf, version),
            ResponseKind::RenewDelegationToken(x) => encode_into(x, buf, version),
            ResponseKind::ExpireDelegationToken(x) => encode_into(x, buf, version),
            ResponseKind::DescribeDelegationToken(x) => encode_into(x, buf, version),
            ResponseKind::DeleteGroups(x) => encode_into(x, buf, version),
            ResponseKind::ElectLeaders(x) => encode_into(x, buf, version),
            ResponseKind::IncrementalAlterConfigs(x) => encode_into(x, buf, version),
            ResponseKind::AlterPartitionReassignments(x) => encode_into(x, buf, version),
            ResponseKind::ListPartitionReassignments(x) => encode_into(x, buf, version),
            ResponseKind::OffsetDelete(x) => encode_into(x, buf, version),
            ResponseKind::DescribeClientQuotas(x) => encode_into(x, buf, version),
            ResponseKind::AlterClientQuotas(x) => encode_into(x, buf, version),
            ResponseKind::DescribeUserScramCredentials(x) => encode_into(x, buf, version),
            ResponseKind::AlterUserScramCredentials(x) => encode_into(x, buf, version),
            ResponseKind::Vote(x) => encode_into(x, buf, version),
            ResponseKind::BeginQuorumEpoch(x) => encode_into(x, buf, version),
            ResponseKind::EndQuorumEpoch(x) => encode_into(x, buf, version),
            ResponseKind::DescribeQuorum(x) => encode_into(x, buf, version),
            ResponseKind::AlterPartition(x) => encode_into(x, buf, version),
            ResponseKind::UpdateFeatures(x) => encode_into(x, buf, version),
            ResponseKind::Envelope(x) => encode_into(x, buf, version),
            ResponseKind::FetchSnapshot(x) => encode_into(x, buf, version),
            ResponseKind::DescribeCluster(x) => encode_into(x, buf, version),
            ResponseKind::DescribeProducers(x) => encode_into(x, buf, version),
            ResponseKind::BrokerRegistration(x) => encode_into(x, buf, version),
            ResponseKind::BrokerHeartbeat(x) => encode_into(x, buf, version),
            ResponseKind::UnregisterBroker(x) => encode_into(x, buf, version),
            ResponseKind::DescribeTransactions(x) => encode_into(x, buf, version),
            ResponseKind::ListTransactions(x) => encode_into(x, buf, version),
            ResponseKind::AllocateProducerIds(x) => encode_into(x, buf, version),
            ResponseKind::ConsumerGroupHeartbeat(x) => encode_into(x, buf, version),
            ResponseKind::ConsumerGroupDescribe(x) => encode_into(x, buf, version),
            ResponseKind::ControllerRegistration(x) => encode_into(x, buf, version),
            ResponseKind::GetTelemetrySubscriptions(x) => encode_into(x, buf, version),
            ResponseKind::PushTelemetry(x) => encode_into(x, buf, version),
            ResponseKind::AssignReplicasToDirs(x) => encode_into(x, buf, version),
            ResponseKind::ListConfigResources(x) => encode_into(x, buf, version),
            ResponseKind::DescribeTopicPartitions(x) => encode_into(x, buf, version),
            ResponseKind::ShareGroupHeartbeat(x) => encode_into(x, buf, version),
            ResponseKind::ShareGroupDescribe(x) => encode_into(x, buf, version),
            ResponseKind::ShareFetch(x) => encode_into(x, buf, version),
            ResponseKind::ShareAcknowledge(x) => encode_into(x, buf, version),
            ResponseKind::AddRaftVoter(x) => encode_into(x, buf, version),
            ResponseKind::RemoveRaftVoter(x) => encode_into(x, buf, version),
            ResponseKind::UpdateRaftVoter(x) => encode_into(x, buf, version),
            ResponseKind::InitializeShareGroupState(x) => encode_into(x, buf, version),
            ResponseKind::ReadShareGroupState(x) => encode_into(x, buf, version),
            ResponseKind::WriteShareGroupState(x) => encode_into(x, buf, version),
            ResponseKind::DeleteShareGroupState(x) => encode_into(x, buf, version),
            ResponseKind::ReadShareGroupStateSummary(x) => encode_into(x, buf, version),
            ResponseKind::DescribeShareGroupOffsets(x) => encode_into(x, buf, version),
            ResponseKind::AlterShareGroupOffsets(x) => encode_into(x, buf, version),
            ResponseKind::DeleteShareGroupOffsets(x) => encode_into(x, buf, version),
        }
    }
    /// Decode the message from the provided buffer and version
    #[cfg(feature = "client")]
    pub fn decode(
        api_key: ApiKey,
        bytes: &mut bytes::Bytes,
        version: i16,
    ) -> crate::error::Result<ResponseKind> {
        match api_key {
            ApiKey::Produce => Ok(ResponseKind::Produce(decode(bytes, version)?)),
            ApiKey::Fetch => Ok(ResponseKind::Fetch(decode(bytes, version)?)),
            ApiKey::ListOffsets => Ok(ResponseKind::ListOffsets(decode(bytes, version)?)),
            ApiKey::Metadata => Ok(ResponseKind::Metadata(decode(bytes, version)?)),
            ApiKey::OffsetCommit => Ok(ResponseKind::OffsetCommit(decode(bytes, version)?)),
            ApiKey::OffsetFetch => Ok(ResponseKind::OffsetFetch(decode(bytes, version)?)),
            ApiKey::FindCoordinator => Ok(ResponseKind::FindCoordinator(decode(bytes, version)?)),
            ApiKey::JoinGroup => Ok(ResponseKind::JoinGroup(decode(bytes, version)?)),
            ApiKey::Heartbeat => Ok(ResponseKind::Heartbeat(decode(bytes, version)?)),
            ApiKey::LeaveGroup => Ok(ResponseKind::LeaveGroup(decode(bytes, version)?)),
            ApiKey::SyncGroup => Ok(ResponseKind::SyncGroup(decode(bytes, version)?)),
            ApiKey::DescribeGroups => Ok(ResponseKind::DescribeGroups(decode(bytes, version)?)),
            ApiKey::ListGroups => Ok(ResponseKind::ListGroups(decode(bytes, version)?)),
            ApiKey::SaslHandshake => Ok(ResponseKind::SaslHandshake(decode(bytes, version)?)),
            ApiKey::ApiVersions => Ok(ResponseKind::ApiVersions(decode(bytes, version)?)),
            ApiKey::CreateTopics => Ok(ResponseKind::CreateTopics(decode(bytes, version)?)),
            ApiKey::DeleteTopics => Ok(ResponseKind::DeleteTopics(decode(bytes, version)?)),
            ApiKey::DeleteRecords => Ok(ResponseKind::DeleteRecords(decode(bytes, version)?)),
            ApiKey::InitProducerId => Ok(ResponseKind::InitProducerId(decode(bytes, version)?)),
            ApiKey::OffsetForLeaderEpoch => {
                Ok(ResponseKind::OffsetForLeaderEpoch(decode(bytes, version)?))
            }
            ApiKey::AddPartitionsToTxn => {
                Ok(ResponseKind::AddPartitionsToTxn(decode(bytes, version)?))
            }
            ApiKey::AddOffsetsToTxn => Ok(ResponseKind::AddOffsetsToTxn(decode(bytes, version)?)),
            ApiKey::EndTxn => Ok(ResponseKind::EndTxn(decode(bytes, version)?)),
            ApiKey::WriteTxnMarkers => Ok(ResponseKind::WriteTxnMarkers(decode(bytes, version)?)),
            ApiKey::TxnOffsetCommit => Ok(ResponseKind::TxnOffsetCommit(decode(bytes, version)?)),
            ApiKey::DescribeAcls => Ok(ResponseKind::DescribeAcls(decode(bytes, version)?)),
            ApiKey::CreateAcls => Ok(ResponseKind::CreateAcls(decode(bytes, version)?)),
            ApiKey::DeleteAcls => Ok(ResponseKind::DeleteAcls(decode(bytes, version)?)),
            ApiKey::DescribeConfigs => Ok(ResponseKind::DescribeConfigs(decode(bytes, version)?)),
            ApiKey::AlterConfigs => Ok(ResponseKind::AlterConfigs(decode(bytes, version)?)),
            ApiKey::AlterReplicaLogDirs => {
                Ok(ResponseKind::AlterReplicaLogDirs(decode(bytes, version)?))
            }
            ApiKey::DescribeLogDirs => Ok(ResponseKind::DescribeLogDirs(decode(bytes, version)?)),
            ApiKey::SaslAuthenticate => Ok(ResponseKind::SaslAuthenticate(decode(bytes, version)?)),
            ApiKey::CreatePartitions => Ok(ResponseKind::CreatePartitions(decode(bytes, version)?)),
            ApiKey::CreateDelegationToken => {
                Ok(ResponseKind::CreateDelegationToken(decode(bytes, version)?))
            }
            ApiKey::RenewDelegationToken => {
                Ok(ResponseKind::RenewDelegationToken(decode(bytes, version)?))
            }
            ApiKey::ExpireDelegationToken => {
                Ok(ResponseKind::ExpireDelegationToken(decode(bytes, version)?))
            }
            ApiKey::DescribeDelegationToken => Ok(ResponseKind::DescribeDelegationToken(decode(
                bytes, version,
            )?)),
            ApiKey::DeleteGroups => Ok(ResponseKind::DeleteGroups(decode(bytes, version)?)),
            ApiKey::ElectLeaders => Ok(ResponseKind::ElectLeaders(decode(bytes, version)?)),
            ApiKey::IncrementalAlterConfigs => Ok(ResponseKind::IncrementalAlterConfigs(decode(
                bytes, version,
            )?)),
            ApiKey::AlterPartitionReassignments => Ok(ResponseKind::AlterPartitionReassignments(
                decode(bytes, version)?,
            )),
            ApiKey::ListPartitionReassignments => Ok(ResponseKind::ListPartitionReassignments(
                decode(bytes, version)?,
            )),
            ApiKey::OffsetDelete => Ok(ResponseKind::OffsetDelete(decode(bytes, version)?)),
            ApiKey::DescribeClientQuotas => {
                Ok(ResponseKind::DescribeClientQuotas(decode(bytes, version)?))
            }
            ApiKey::AlterClientQuotas => {
                Ok(ResponseKind::AlterClientQuotas(decode(bytes, version)?))
            }
            ApiKey::DescribeUserScramCredentials => Ok(ResponseKind::DescribeUserScramCredentials(
                decode(bytes, version)?,
            )),
            ApiKey::AlterUserScramCredentials => Ok(ResponseKind::AlterUserScramCredentials(
                decode(bytes, version)?,
            )),
            ApiKey::Vote => Ok(ResponseKind::Vote(decode(bytes, version)?)),
            ApiKey::BeginQuorumEpoch => Ok(ResponseKind::BeginQuorumEpoch(decode(bytes, version)?)),
            ApiKey::EndQuorumEpoch => Ok(ResponseKind::EndQuorumEpoch(decode(bytes, version)?)),
            ApiKey::DescribeQuorum => Ok(ResponseKind::DescribeQuorum(decode(bytes, version)?)),
            ApiKey::AlterPartition => Ok(ResponseKind::AlterPartition(decode(bytes, version)?)),
            ApiKey::UpdateFeatures => Ok(ResponseKind::UpdateFeatures(decode(bytes, version)?)),
            ApiKey::Envelope => Ok(ResponseKind::Envelope(decode(bytes, version)?)),
            ApiKey::FetchSnapshot => Ok(ResponseKind::FetchSnapshot(decode(bytes, version)?)),
            ApiKey::DescribeCluster => Ok(ResponseKind::DescribeCluster(decode(bytes, version)?)),
            ApiKey::DescribeProducers => {
                Ok(ResponseKind::DescribeProducers(decode(bytes, version)?))
            }
            ApiKey::BrokerRegistration => {
                Ok(ResponseKind::BrokerRegistration(decode(bytes, version)?))
            }
            ApiKey::BrokerHeartbeat => Ok(ResponseKind::BrokerHeartbeat(decode(bytes, version)?)),
            ApiKey::UnregisterBroker => Ok(ResponseKind::UnregisterBroker(decode(bytes, version)?)),
            ApiKey::DescribeTransactions => {
                Ok(ResponseKind::DescribeTransactions(decode(bytes, version)?))
            }
            ApiKey::ListTransactions => Ok(ResponseKind::ListTransactions(decode(bytes, version)?)),
            ApiKey::AllocateProducerIds => {
                Ok(ResponseKind::AllocateProducerIds(decode(bytes, version)?))
            }
            ApiKey::ConsumerGroupHeartbeat => Ok(ResponseKind::ConsumerGroupHeartbeat(decode(
                bytes, version,
            )?)),
            ApiKey::ConsumerGroupDescribe => {
                Ok(ResponseKind::ConsumerGroupDescribe(decode(bytes, version)?))
            }
            ApiKey::ControllerRegistration => Ok(ResponseKind::ControllerRegistration(decode(
                bytes, version,
            )?)),
            ApiKey::GetTelemetrySubscriptions => Ok(ResponseKind::GetTelemetrySubscriptions(
                decode(bytes, version)?,
            )),
            ApiKey::PushTelemetry => Ok(ResponseKind::PushTelemetry(decode(bytes, version)?)),
            ApiKey::AssignReplicasToDirs => {
                Ok(ResponseKind::AssignReplicasToDirs(decode(bytes, version)?))
            }
            ApiKey::ListConfigResources => {
                Ok(ResponseKind::ListConfigResources(decode(bytes, version)?))
            }
            ApiKey::DescribeTopicPartitions => Ok(ResponseKind::DescribeTopicPartitions(decode(
                bytes, version,
            )?)),
            ApiKey::ShareGroupHeartbeat => {
                Ok(ResponseKind::ShareGroupHeartbeat(decode(bytes, version)?))
            }
            ApiKey::ShareGroupDescribe => {
                Ok(ResponseKind::ShareGroupDescribe(decode(bytes, version)?))
            }
            ApiKey::ShareFetch => Ok(ResponseKind::ShareFetch(decode(bytes, version)?)),
            ApiKey::ShareAcknowledge => Ok(ResponseKind::ShareAcknowledge(decode(bytes, version)?)),
            ApiKey::AddRaftVoter => Ok(ResponseKind::AddRaftVoter(decode(bytes, version)?)),
            ApiKey::RemoveRaftVoter => Ok(ResponseKind::RemoveRaftVoter(decode(bytes, version)?)),
            ApiKey::UpdateRaftVoter => Ok(ResponseKind::UpdateRaftVoter(decode(bytes, version)?)),
            ApiKey::InitializeShareGroupState => Ok(ResponseKind::InitializeShareGroupState(
                decode(bytes, version)?,
            )),
            ApiKey::ReadShareGroupState => {
                Ok(ResponseKind::ReadShareGroupState(decode(bytes, version)?))
            }
            ApiKey::WriteShareGroupState => {
                Ok(ResponseKind::WriteShareGroupState(decode(bytes, version)?))
            }
            ApiKey::DeleteShareGroupState => {
                Ok(ResponseKind::DeleteShareGroupState(decode(bytes, version)?))
            }
            ApiKey::ReadShareGroupStateSummary => Ok(ResponseKind::ReadShareGroupStateSummary(
                decode(bytes, version)?,
            )),
            ApiKey::DescribeShareGroupOffsets => Ok(ResponseKind::DescribeShareGroupOffsets(
                decode(bytes, version)?,
            )),
            ApiKey::AlterShareGroupOffsets => Ok(ResponseKind::AlterShareGroupOffsets(decode(
                bytes, version,
            )?)),
            ApiKey::DeleteShareGroupOffsets => Ok(ResponseKind::DeleteShareGroupOffsets(decode(
                bytes, version,
            )?)),
        }
    }
    /// Get the version of request header that needs to be prepended to this message
    pub fn header_version(&self, version: i16) -> i16 {
        match self {
            ResponseKind::Produce(_) => ProduceResponse::header_version(version),
            ResponseKind::Fetch(_) => FetchResponse::header_version(version),
            ResponseKind::ListOffsets(_) => ListOffsetsResponse::header_version(version),
            ResponseKind::Metadata(_) => MetadataResponse::header_version(version),
            ResponseKind::OffsetCommit(_) => OffsetCommitResponse::header_version(version),
            ResponseKind::OffsetFetch(_) => OffsetFetchResponse::header_version(version),
            ResponseKind::FindCoordinator(_) => FindCoordinatorResponse::header_version(version),
            ResponseKind::JoinGroup(_) => JoinGroupResponse::header_version(version),
            ResponseKind::Heartbeat(_) => HeartbeatResponse::header_version(version),
            ResponseKind::LeaveGroup(_) => LeaveGroupResponse::header_version(version),
            ResponseKind::SyncGroup(_) => SyncGroupResponse::header_version(version),
            ResponseKind::DescribeGroups(_) => DescribeGroupsResponse::header_version(version),
            ResponseKind::ListGroups(_) => ListGroupsResponse::header_version(version),
            ResponseKind::SaslHandshake(_) => SaslHandshakeResponse::header_version(version),
            ResponseKind::ApiVersions(_) => ApiVersionsResponse::header_version(version),
            ResponseKind::CreateTopics(_) => CreateTopicsResponse::header_version(version),
            ResponseKind::DeleteTopics(_) => DeleteTopicsResponse::header_version(version),
            ResponseKind::DeleteRecords(_) => DeleteRecordsResponse::header_version(version),
            ResponseKind::InitProducerId(_) => InitProducerIdResponse::header_version(version),
            ResponseKind::OffsetForLeaderEpoch(_) => {
                OffsetForLeaderEpochResponse::header_version(version)
            }
            ResponseKind::AddPartitionsToTxn(_) => {
                AddPartitionsToTxnResponse::header_version(version)
            }
            ResponseKind::AddOffsetsToTxn(_) => AddOffsetsToTxnResponse::header_version(version),
            ResponseKind::EndTxn(_) => EndTxnResponse::header_version(version),
            ResponseKind::WriteTxnMarkers(_) => WriteTxnMarkersResponse::header_version(version),
            ResponseKind::TxnOffsetCommit(_) => TxnOffsetCommitResponse::header_version(version),
            ResponseKind::DescribeAcls(_) => DescribeAclsResponse::header_version(version),
            ResponseKind::CreateAcls(_) => CreateAclsResponse::header_version(version),
            ResponseKind::DeleteAcls(_) => DeleteAclsResponse::header_version(version),
            ResponseKind::DescribeConfigs(_) => DescribeConfigsResponse::header_version(version),
            ResponseKind::AlterConfigs(_) => AlterConfigsResponse::header_version(version),
            ResponseKind::AlterReplicaLogDirs(_) => {
                AlterReplicaLogDirsResponse::header_version(version)
            }
            ResponseKind::DescribeLogDirs(_) => DescribeLogDirsResponse::header_version(version),
            ResponseKind::SaslAuthenticate(_) => SaslAuthenticateResponse::header_version(version),
            ResponseKind::CreatePartitions(_) => CreatePartitionsResponse::header_version(version),
            ResponseKind::CreateDelegationToken(_) => {
                CreateDelegationTokenResponse::header_version(version)
            }
            ResponseKind::RenewDelegationToken(_) => {
                RenewDelegationTokenResponse::header_version(version)
            }
            ResponseKind::ExpireDelegationToken(_) => {
                ExpireDelegationTokenResponse::header_version(version)
            }
            ResponseKind::DescribeDelegationToken(_) => {
                DescribeDelegationTokenResponse::header_version(version)
            }
            ResponseKind::DeleteGroups(_) => DeleteGroupsResponse::header_version(version),
            ResponseKind::ElectLeaders(_) => ElectLeadersResponse::header_version(version),
            ResponseKind::IncrementalAlterConfigs(_) => {
                IncrementalAlterConfigsResponse::header_version(version)
            }
            ResponseKind::AlterPartitionReassignments(_) => {
                AlterPartitionReassignmentsResponse::header_version(version)
            }
            ResponseKind::ListPartitionReassignments(_) => {
                ListPartitionReassignmentsResponse::header_version(version)
            }
            ResponseKind::OffsetDelete(_) => OffsetDeleteResponse::header_version(version),
            ResponseKind::DescribeClientQuotas(_) => {
                DescribeClientQuotasResponse::header_version(version)
            }
            ResponseKind::AlterClientQuotas(_) => {
                AlterClientQuotasResponse::header_version(version)
            }
            ResponseKind::DescribeUserScramCredentials(_) => {
                DescribeUserScramCredentialsResponse::header_version(version)
            }
            ResponseKind::AlterUserScramCredentials(_) => {
                AlterUserScramCredentialsResponse::header_version(version)
            }
            ResponseKind::Vote(_) => VoteResponse::header_version(version),
            ResponseKind::BeginQuorumEpoch(_) => BeginQuorumEpochResponse::header_version(version),
            ResponseKind::EndQuorumEpoch(_) => EndQuorumEpochResponse::header_version(version),
            ResponseKind::DescribeQuorum(_) => DescribeQuorumResponse::header_version(version),
            ResponseKind::AlterPartition(_) => AlterPartitionResponse::header_version(version),
            ResponseKind::UpdateFeatures(_) => UpdateFeaturesResponse::header_version(version),
            ResponseKind::Envelope(_) => EnvelopeResponse::header_version(version),
            ResponseKind::FetchSnapshot(_) => FetchSnapshotResponse::header_version(version),
            ResponseKind::DescribeCluster(_) => DescribeClusterResponse::header_version(version),
            ResponseKind::DescribeProducers(_) => {
                DescribeProducersResponse::header_version(version)
            }
            ResponseKind::BrokerRegistration(_) => {
                BrokerRegistrationResponse::header_version(version)
            }
            ResponseKind::BrokerHeartbeat(_) => BrokerHeartbeatResponse::header_version(version),
            ResponseKind::UnregisterBroker(_) => UnregisterBrokerResponse::header_version(version),
            ResponseKind::DescribeTransactions(_) => {
                DescribeTransactionsResponse::header_version(version)
            }
            ResponseKind::ListTransactions(_) => ListTransactionsResponse::header_version(version),
            ResponseKind::AllocateProducerIds(_) => {
                AllocateProducerIdsResponse::header_version(version)
            }
            ResponseKind::ConsumerGroupHeartbeat(_) => {
                ConsumerGroupHeartbeatResponse::header_version(version)
            }
            ResponseKind::ConsumerGroupDescribe(_) => {
                ConsumerGroupDescribeResponse::header_version(version)
            }
            ResponseKind::ControllerRegistration(_) => {
                ControllerRegistrationResponse::header_version(version)
            }
            ResponseKind::GetTelemetrySubscriptions(_) => {
                GetTelemetrySubscriptionsResponse::header_version(version)
            }
            ResponseKind::PushTelemetry(_) => PushTelemetryResponse::header_version(version),
            ResponseKind::AssignReplicasToDirs(_) => {
                AssignReplicasToDirsResponse::header_version(version)
            }
            ResponseKind::ListConfigResources(_) => {
                ListConfigResourcesResponse::header_version(version)
            }
            ResponseKind::DescribeTopicPartitions(_) => {
                DescribeTopicPartitionsResponse::header_version(version)
            }
            ResponseKind::ShareGroupHeartbeat(_) => {
                ShareGroupHeartbeatResponse::header_version(version)
            }
            ResponseKind::ShareGroupDescribe(_) => {
                ShareGroupDescribeResponse::header_version(version)
            }
            ResponseKind::ShareFetch(_) => ShareFetchResponse::header_version(version),
            ResponseKind::ShareAcknowledge(_) => ShareAcknowledgeResponse::header_version(version),
            ResponseKind::AddRaftVoter(_) => AddRaftVoterResponse::header_version(version),
            ResponseKind::RemoveRaftVoter(_) => RemoveRaftVoterResponse::header_version(version),
            ResponseKind::UpdateRaftVoter(_) => UpdateRaftVoterResponse::header_version(version),
            ResponseKind::InitializeShareGroupState(_) => {
                InitializeShareGroupStateResponse::header_version(version)
            }
            ResponseKind::ReadShareGroupState(_) => {
                ReadShareGroupStateResponse::header_version(version)
            }
            ResponseKind::WriteShareGroupState(_) => {
                WriteShareGroupStateResponse::header_version(version)
            }
            ResponseKind::DeleteShareGroupState(_) => {
                DeleteShareGroupStateResponse::header_version(version)
            }
            ResponseKind::ReadShareGroupStateSummary(_) => {
                ReadShareGroupStateSummaryResponse::header_version(version)
            }
            ResponseKind::DescribeShareGroupOffsets(_) => {
                DescribeShareGroupOffsetsResponse::header_version(version)
            }
            ResponseKind::AlterShareGroupOffsets(_) => {
                AlterShareGroupOffsetsResponse::header_version(version)
            }
            ResponseKind::DeleteShareGroupOffsets(_) => {
                DeleteShareGroupOffsetsResponse::header_version(version)
            }
        }
    }
}

#[cfg(feature = "messages_enums")]
impl From<ProduceResponse> for ResponseKind {
    fn from(value: ProduceResponse) -> ResponseKind {
        ResponseKind::Produce(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FetchResponse> for ResponseKind {
    fn from(value: FetchResponse) -> ResponseKind {
        ResponseKind::Fetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListOffsetsResponse> for ResponseKind {
    fn from(value: ListOffsetsResponse) -> ResponseKind {
        ResponseKind::ListOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<MetadataResponse> for ResponseKind {
    fn from(value: MetadataResponse) -> ResponseKind {
        ResponseKind::Metadata(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetCommitResponse> for ResponseKind {
    fn from(value: OffsetCommitResponse) -> ResponseKind {
        ResponseKind::OffsetCommit(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetFetchResponse> for ResponseKind {
    fn from(value: OffsetFetchResponse) -> ResponseKind {
        ResponseKind::OffsetFetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FindCoordinatorResponse> for ResponseKind {
    fn from(value: FindCoordinatorResponse) -> ResponseKind {
        ResponseKind::FindCoordinator(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<JoinGroupResponse> for ResponseKind {
    fn from(value: JoinGroupResponse) -> ResponseKind {
        ResponseKind::JoinGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<HeartbeatResponse> for ResponseKind {
    fn from(value: HeartbeatResponse) -> ResponseKind {
        ResponseKind::Heartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<LeaveGroupResponse> for ResponseKind {
    fn from(value: LeaveGroupResponse) -> ResponseKind {
        ResponseKind::LeaveGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SyncGroupResponse> for ResponseKind {
    fn from(value: SyncGroupResponse) -> ResponseKind {
        ResponseKind::SyncGroup(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeGroupsResponse> for ResponseKind {
    fn from(value: DescribeGroupsResponse) -> ResponseKind {
        ResponseKind::DescribeGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListGroupsResponse> for ResponseKind {
    fn from(value: ListGroupsResponse) -> ResponseKind {
        ResponseKind::ListGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SaslHandshakeResponse> for ResponseKind {
    fn from(value: SaslHandshakeResponse) -> ResponseKind {
        ResponseKind::SaslHandshake(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ApiVersionsResponse> for ResponseKind {
    fn from(value: ApiVersionsResponse) -> ResponseKind {
        ResponseKind::ApiVersions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateTopicsResponse> for ResponseKind {
    fn from(value: CreateTopicsResponse) -> ResponseKind {
        ResponseKind::CreateTopics(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteTopicsResponse> for ResponseKind {
    fn from(value: DeleteTopicsResponse) -> ResponseKind {
        ResponseKind::DeleteTopics(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteRecordsResponse> for ResponseKind {
    fn from(value: DeleteRecordsResponse) -> ResponseKind {
        ResponseKind::DeleteRecords(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<InitProducerIdResponse> for ResponseKind {
    fn from(value: InitProducerIdResponse) -> ResponseKind {
        ResponseKind::InitProducerId(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetForLeaderEpochResponse> for ResponseKind {
    fn from(value: OffsetForLeaderEpochResponse) -> ResponseKind {
        ResponseKind::OffsetForLeaderEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddPartitionsToTxnResponse> for ResponseKind {
    fn from(value: AddPartitionsToTxnResponse) -> ResponseKind {
        ResponseKind::AddPartitionsToTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddOffsetsToTxnResponse> for ResponseKind {
    fn from(value: AddOffsetsToTxnResponse) -> ResponseKind {
        ResponseKind::AddOffsetsToTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EndTxnResponse> for ResponseKind {
    fn from(value: EndTxnResponse) -> ResponseKind {
        ResponseKind::EndTxn(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<WriteTxnMarkersResponse> for ResponseKind {
    fn from(value: WriteTxnMarkersResponse) -> ResponseKind {
        ResponseKind::WriteTxnMarkers(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<TxnOffsetCommitResponse> for ResponseKind {
    fn from(value: TxnOffsetCommitResponse) -> ResponseKind {
        ResponseKind::TxnOffsetCommit(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeAclsResponse> for ResponseKind {
    fn from(value: DescribeAclsResponse) -> ResponseKind {
        ResponseKind::DescribeAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateAclsResponse> for ResponseKind {
    fn from(value: CreateAclsResponse) -> ResponseKind {
        ResponseKind::CreateAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteAclsResponse> for ResponseKind {
    fn from(value: DeleteAclsResponse) -> ResponseKind {
        ResponseKind::DeleteAcls(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeConfigsResponse> for ResponseKind {
    fn from(value: DescribeConfigsResponse) -> ResponseKind {
        ResponseKind::DescribeConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterConfigsResponse> for ResponseKind {
    fn from(value: AlterConfigsResponse) -> ResponseKind {
        ResponseKind::AlterConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterReplicaLogDirsResponse> for ResponseKind {
    fn from(value: AlterReplicaLogDirsResponse) -> ResponseKind {
        ResponseKind::AlterReplicaLogDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeLogDirsResponse> for ResponseKind {
    fn from(value: DescribeLogDirsResponse) -> ResponseKind {
        ResponseKind::DescribeLogDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<SaslAuthenticateResponse> for ResponseKind {
    fn from(value: SaslAuthenticateResponse) -> ResponseKind {
        ResponseKind::SaslAuthenticate(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreatePartitionsResponse> for ResponseKind {
    fn from(value: CreatePartitionsResponse) -> ResponseKind {
        ResponseKind::CreatePartitions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<CreateDelegationTokenResponse> for ResponseKind {
    fn from(value: CreateDelegationTokenResponse) -> ResponseKind {
        ResponseKind::CreateDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<RenewDelegationTokenResponse> for ResponseKind {
    fn from(value: RenewDelegationTokenResponse) -> ResponseKind {
        ResponseKind::RenewDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ExpireDelegationTokenResponse> for ResponseKind {
    fn from(value: ExpireDelegationTokenResponse) -> ResponseKind {
        ResponseKind::ExpireDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeDelegationTokenResponse> for ResponseKind {
    fn from(value: DescribeDelegationTokenResponse) -> ResponseKind {
        ResponseKind::DescribeDelegationToken(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteGroupsResponse> for ResponseKind {
    fn from(value: DeleteGroupsResponse) -> ResponseKind {
        ResponseKind::DeleteGroups(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ElectLeadersResponse> for ResponseKind {
    fn from(value: ElectLeadersResponse) -> ResponseKind {
        ResponseKind::ElectLeaders(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<IncrementalAlterConfigsResponse> for ResponseKind {
    fn from(value: IncrementalAlterConfigsResponse) -> ResponseKind {
        ResponseKind::IncrementalAlterConfigs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterPartitionReassignmentsResponse> for ResponseKind {
    fn from(value: AlterPartitionReassignmentsResponse) -> ResponseKind {
        ResponseKind::AlterPartitionReassignments(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListPartitionReassignmentsResponse> for ResponseKind {
    fn from(value: ListPartitionReassignmentsResponse) -> ResponseKind {
        ResponseKind::ListPartitionReassignments(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<OffsetDeleteResponse> for ResponseKind {
    fn from(value: OffsetDeleteResponse) -> ResponseKind {
        ResponseKind::OffsetDelete(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeClientQuotasResponse> for ResponseKind {
    fn from(value: DescribeClientQuotasResponse) -> ResponseKind {
        ResponseKind::DescribeClientQuotas(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterClientQuotasResponse> for ResponseKind {
    fn from(value: AlterClientQuotasResponse) -> ResponseKind {
        ResponseKind::AlterClientQuotas(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeUserScramCredentialsResponse> for ResponseKind {
    fn from(value: DescribeUserScramCredentialsResponse) -> ResponseKind {
        ResponseKind::DescribeUserScramCredentials(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterUserScramCredentialsResponse> for ResponseKind {
    fn from(value: AlterUserScramCredentialsResponse) -> ResponseKind {
        ResponseKind::AlterUserScramCredentials(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<VoteResponse> for ResponseKind {
    fn from(value: VoteResponse) -> ResponseKind {
        ResponseKind::Vote(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BeginQuorumEpochResponse> for ResponseKind {
    fn from(value: BeginQuorumEpochResponse) -> ResponseKind {
        ResponseKind::BeginQuorumEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EndQuorumEpochResponse> for ResponseKind {
    fn from(value: EndQuorumEpochResponse) -> ResponseKind {
        ResponseKind::EndQuorumEpoch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeQuorumResponse> for ResponseKind {
    fn from(value: DescribeQuorumResponse) -> ResponseKind {
        ResponseKind::DescribeQuorum(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterPartitionResponse> for ResponseKind {
    fn from(value: AlterPartitionResponse) -> ResponseKind {
        ResponseKind::AlterPartition(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UpdateFeaturesResponse> for ResponseKind {
    fn from(value: UpdateFeaturesResponse) -> ResponseKind {
        ResponseKind::UpdateFeatures(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<EnvelopeResponse> for ResponseKind {
    fn from(value: EnvelopeResponse) -> ResponseKind {
        ResponseKind::Envelope(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<FetchSnapshotResponse> for ResponseKind {
    fn from(value: FetchSnapshotResponse) -> ResponseKind {
        ResponseKind::FetchSnapshot(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeClusterResponse> for ResponseKind {
    fn from(value: DescribeClusterResponse) -> ResponseKind {
        ResponseKind::DescribeCluster(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeProducersResponse> for ResponseKind {
    fn from(value: DescribeProducersResponse) -> ResponseKind {
        ResponseKind::DescribeProducers(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BrokerRegistrationResponse> for ResponseKind {
    fn from(value: BrokerRegistrationResponse) -> ResponseKind {
        ResponseKind::BrokerRegistration(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<BrokerHeartbeatResponse> for ResponseKind {
    fn from(value: BrokerHeartbeatResponse) -> ResponseKind {
        ResponseKind::BrokerHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UnregisterBrokerResponse> for ResponseKind {
    fn from(value: UnregisterBrokerResponse) -> ResponseKind {
        ResponseKind::UnregisterBroker(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeTransactionsResponse> for ResponseKind {
    fn from(value: DescribeTransactionsResponse) -> ResponseKind {
        ResponseKind::DescribeTransactions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListTransactionsResponse> for ResponseKind {
    fn from(value: ListTransactionsResponse) -> ResponseKind {
        ResponseKind::ListTransactions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AllocateProducerIdsResponse> for ResponseKind {
    fn from(value: AllocateProducerIdsResponse) -> ResponseKind {
        ResponseKind::AllocateProducerIds(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ConsumerGroupHeartbeatResponse> for ResponseKind {
    fn from(value: ConsumerGroupHeartbeatResponse) -> ResponseKind {
        ResponseKind::ConsumerGroupHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ConsumerGroupDescribeResponse> for ResponseKind {
    fn from(value: ConsumerGroupDescribeResponse) -> ResponseKind {
        ResponseKind::ConsumerGroupDescribe(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ControllerRegistrationResponse> for ResponseKind {
    fn from(value: ControllerRegistrationResponse) -> ResponseKind {
        ResponseKind::ControllerRegistration(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<GetTelemetrySubscriptionsResponse> for ResponseKind {
    fn from(value: GetTelemetrySubscriptionsResponse) -> ResponseKind {
        ResponseKind::GetTelemetrySubscriptions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<PushTelemetryResponse> for ResponseKind {
    fn from(value: PushTelemetryResponse) -> ResponseKind {
        ResponseKind::PushTelemetry(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AssignReplicasToDirsResponse> for ResponseKind {
    fn from(value: AssignReplicasToDirsResponse) -> ResponseKind {
        ResponseKind::AssignReplicasToDirs(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ListConfigResourcesResponse> for ResponseKind {
    fn from(value: ListConfigResourcesResponse) -> ResponseKind {
        ResponseKind::ListConfigResources(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeTopicPartitionsResponse> for ResponseKind {
    fn from(value: DescribeTopicPartitionsResponse) -> ResponseKind {
        ResponseKind::DescribeTopicPartitions(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareGroupHeartbeatResponse> for ResponseKind {
    fn from(value: ShareGroupHeartbeatResponse) -> ResponseKind {
        ResponseKind::ShareGroupHeartbeat(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareGroupDescribeResponse> for ResponseKind {
    fn from(value: ShareGroupDescribeResponse) -> ResponseKind {
        ResponseKind::ShareGroupDescribe(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareFetchResponse> for ResponseKind {
    fn from(value: ShareFetchResponse) -> ResponseKind {
        ResponseKind::ShareFetch(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ShareAcknowledgeResponse> for ResponseKind {
    fn from(value: ShareAcknowledgeResponse) -> ResponseKind {
        ResponseKind::ShareAcknowledge(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AddRaftVoterResponse> for ResponseKind {
    fn from(value: AddRaftVoterResponse) -> ResponseKind {
        ResponseKind::AddRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<RemoveRaftVoterResponse> for ResponseKind {
    fn from(value: RemoveRaftVoterResponse) -> ResponseKind {
        ResponseKind::RemoveRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<UpdateRaftVoterResponse> for ResponseKind {
    fn from(value: UpdateRaftVoterResponse) -> ResponseKind {
        ResponseKind::UpdateRaftVoter(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<InitializeShareGroupStateResponse> for ResponseKind {
    fn from(value: InitializeShareGroupStateResponse) -> ResponseKind {
        ResponseKind::InitializeShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ReadShareGroupStateResponse> for ResponseKind {
    fn from(value: ReadShareGroupStateResponse) -> ResponseKind {
        ResponseKind::ReadShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<WriteShareGroupStateResponse> for ResponseKind {
    fn from(value: WriteShareGroupStateResponse) -> ResponseKind {
        ResponseKind::WriteShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteShareGroupStateResponse> for ResponseKind {
    fn from(value: DeleteShareGroupStateResponse) -> ResponseKind {
        ResponseKind::DeleteShareGroupState(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<ReadShareGroupStateSummaryResponse> for ResponseKind {
    fn from(value: ReadShareGroupStateSummaryResponse) -> ResponseKind {
        ResponseKind::ReadShareGroupStateSummary(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DescribeShareGroupOffsetsResponse> for ResponseKind {
    fn from(value: DescribeShareGroupOffsetsResponse) -> ResponseKind {
        ResponseKind::DescribeShareGroupOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<AlterShareGroupOffsetsResponse> for ResponseKind {
    fn from(value: AlterShareGroupOffsetsResponse) -> ResponseKind {
        ResponseKind::AlterShareGroupOffsets(value)
    }
}

#[cfg(feature = "messages_enums")]
impl From<DeleteShareGroupOffsetsResponse> for ResponseKind {
    fn from(value: DeleteShareGroupOffsetsResponse) -> ResponseKind {
        ResponseKind::DeleteShareGroupOffsets(value)
    }
}

/// The replica id of the current leader or -1 if the leader is unknown.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Copy)]
pub struct BrokerId(pub i32);

impl From<i32> for BrokerId {
    fn from(other: i32) -> Self {
        Self(other)
    }
}
impl From<BrokerId> for i32 {
    fn from(other: BrokerId) -> Self {
        other.0
    }
}
impl std::borrow::Borrow<i32> for BrokerId {
    fn borrow(&self) -> &i32 {
        &self.0
    }
}
impl std::ops::Deref for BrokerId {
    type Target = i32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::cmp::PartialEq<i32> for BrokerId {
    fn eq(&self, other: &i32) -> bool {
        &self.0 == other
    }
}
impl std::cmp::PartialEq<BrokerId> for i32 {
    fn eq(&self, other: &BrokerId) -> bool {
        self == &other.0
    }
}
impl std::fmt::Debug for BrokerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl NewType<i32> for BrokerId {}

/// The group identifier.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct GroupId(pub StrBytes);

impl From<StrBytes> for GroupId {
    fn from(other: StrBytes) -> Self {
        Self(other)
    }
}
impl From<GroupId> for StrBytes {
    fn from(other: GroupId) -> Self {
        other.0
    }
}
impl std::borrow::Borrow<StrBytes> for GroupId {
    fn borrow(&self) -> &StrBytes {
        &self.0
    }
}
impl std::ops::Deref for GroupId {
    type Target = StrBytes;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::cmp::PartialEq<StrBytes> for GroupId {
    fn eq(&self, other: &StrBytes) -> bool {
        &self.0 == other
    }
}
impl std::cmp::PartialEq<GroupId> for StrBytes {
    fn eq(&self, other: &GroupId) -> bool {
        self == &other.0
    }
}
impl std::fmt::Debug for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl NewType<StrBytes> for GroupId {}

/// The first producer ID in this range, inclusive.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Copy)]
pub struct ProducerId(pub i64);

impl From<i64> for ProducerId {
    fn from(other: i64) -> Self {
        Self(other)
    }
}
impl From<ProducerId> for i64 {
    fn from(other: ProducerId) -> Self {
        other.0
    }
}
impl std::borrow::Borrow<i64> for ProducerId {
    fn borrow(&self) -> &i64 {
        &self.0
    }
}
impl std::ops::Deref for ProducerId {
    type Target = i64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::cmp::PartialEq<i64> for ProducerId {
    fn eq(&self, other: &i64) -> bool {
        &self.0 == other
    }
}
impl std::cmp::PartialEq<ProducerId> for i64 {
    fn eq(&self, other: &ProducerId) -> bool {
        self == &other.0
    }
}
impl std::fmt::Debug for ProducerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl NewType<i64> for ProducerId {}

/// The topic name.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct TopicName(pub StrBytes);

impl From<StrBytes> for TopicName {
    fn from(other: StrBytes) -> Self {
        Self(other)
    }
}
impl From<TopicName> for StrBytes {
    fn from(other: TopicName) -> Self {
        other.0
    }
}
impl std::borrow::Borrow<StrBytes> for TopicName {
    fn borrow(&self) -> &StrBytes {
        &self.0
    }
}
impl std::ops::Deref for TopicName {
    type Target = StrBytes;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::cmp::PartialEq<StrBytes> for TopicName {
    fn eq(&self, other: &StrBytes) -> bool {
        &self.0 == other
    }
}
impl std::cmp::PartialEq<TopicName> for StrBytes {
    fn eq(&self, other: &TopicName) -> bool {
        self == &other.0
    }
}
impl std::fmt::Debug for TopicName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl NewType<StrBytes> for TopicName {}

/// The transactional id.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct TransactionalId(pub StrBytes);

impl From<StrBytes> for TransactionalId {
    fn from(other: StrBytes) -> Self {
        Self(other)
    }
}
impl From<TransactionalId> for StrBytes {
    fn from(other: TransactionalId) -> Self {
        other.0
    }
}
impl std::borrow::Borrow<StrBytes> for TransactionalId {
    fn borrow(&self) -> &StrBytes {
        &self.0
    }
}
impl std::ops::Deref for TransactionalId {
    type Target = StrBytes;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::cmp::PartialEq<StrBytes> for TransactionalId {
    fn eq(&self, other: &StrBytes) -> bool {
        &self.0 == other
    }
}
impl std::cmp::PartialEq<TransactionalId> for StrBytes {
    fn eq(&self, other: &TransactionalId) -> bool {
        self == &other.0
    }
}
impl std::fmt::Debug for TransactionalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl NewType<StrBytes> for TransactionalId {}
