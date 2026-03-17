//! Error kinds for Kafka error codes.

use paste::paste;

/// A trait providing tools for parsing Kafka response error code.
pub trait ParseResponseErrorCode {
    /// Convert from an i16 error code to `Option<()>`, if is ok, returns `Some(())`.
    ///
    /// Convert self into an Option<()>, consuming self, and discarding the error code, if any.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use proto_kafka::error::ParseResponseErrorCode;
    ///
    /// assert_eq!(0.ok(), Some(()));
    /// assert_eq!((-1).ok(), None);
    /// assert_eq!(100.ok(), None);
    /// assert_eq!(1000.ok(), None);
    /// ```
    fn ok(self) -> Option<()>;

    /// Convert from an i16 error code to `Option<ResponseError>`, if is error, returns `Some(ResponseError)`
    ///
    /// Convert self into an Option<ResponseError>, consuming self, and discarding the success
    /// value, if any.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use proto_kafka::error::{
    ///     ParseResponseErrorCode,
    ///     ResponseError,
    /// };
    ///
    /// assert_eq!(0.err(), None);
    /// assert_eq!((-1).err(), Some(ResponseError::UnknownServerError));
    /// assert_eq!(100.err(), Some(ResponseError::UnknownTopicId));
    /// assert_eq!(1000.err(), Some(ResponseError::Unknown(1000)));
    /// ```
    fn err(self) -> Option<ResponseError>;

    /// Returns true if the result is ok.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use proto_kafka::error::ParseResponseErrorCode;
    ///
    /// assert_eq!(0.is_ok(), true);
    /// assert_eq!((-1).is_ok(), false);
    /// assert_eq!(100.is_ok(), false);
    /// assert_eq!(1000.is_ok(), false);
    /// ```
    fn is_ok(&self) -> bool;

    /// Returns true if the result is error.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use proto_kafka::error::ParseResponseErrorCode;
    ///
    /// assert_eq!(0.is_err(), false);
    /// assert_eq!((-1).is_err(), true);
    /// assert_eq!(100.is_err(), true);
    /// assert_eq!(1000.is_err(), true);
    /// ```
    fn is_err(&self) -> bool;
}

impl ParseResponseErrorCode for i16 {
    fn ok(self) -> Option<()> {
        if self == 0 {
            Some(())
        } else {
            None
        }
    }

    fn err(self) -> Option<ResponseError> {
        ResponseError::try_from_code(self)
    }

    fn is_ok(&self) -> bool {
        *self == 0
    }

    fn is_err(&self) -> bool {
        *self != 0
    }
}

macro_rules! define_errors {
    ( $name:ident, $( ( $kind:ident, $code:literal, $retriable:literal, $desc:literal ) ),* $(,)? ) => {

        paste!{
            /// Error kinds for Kafka error codes.
            ///
            /// This enum type contains all the kafka client-server errors, those errors that must be sent from the server to the client.
            /// These are thus **part of the protocol**. The names can be changed but the error code cannot.
            ///
            /// Note that client library will convert an unknown error code to the non-retriable variant if the client library
            /// version is old and does not recognize the newly-added error code. Therefore when a new server-side error is added,
            /// Kafka server may need extra logic to convert the new error code to another existing error code before sending the response back to
            /// the client if the request version suggests that the client may not recognize the new error code.
            ///
            /// Read [Kafka Protocol Guide](https://kafka.apache.org/protocol#protocol_error_codes) and
            /// [Errors.java](https://github.com/apache/kafka/blob/6d7723f073/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L135-L147) for more details.
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub enum $name {

                /// Client-side unknown error code.
                Unknown(i16),

                $(
                    #[doc = $desc]
                    [<$kind:camel>],
                )*
            }

            impl std::error::Error for $name {}

            impl std::fmt::Display for $name {
                fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    match self.try_as_str() {
                        Ok(s) => f.write_str(s),
                        Err(code) => f.write_fmt(format_args!("Unknown error code: {}", code)),
                    }
                }
            }

            impl $name {

                #[doc = concat!("Try to convert an `i16` Kafka error code to an `", stringify!($name), "` variant.")]
                #[doc = ""]
                #[doc = "Returns `None` if the error `code` is `0`."]
                #[doc = concat!("Otherwise, the `Some(", stringify!($name), ")` is returned.")]
                pub fn try_from_code(code: i16) -> Option<Self> {
                    match code {
                        0 => None,
                        $(
                            $code => Some($name::[<$kind:camel>]),
                        )*
                        _ => Some(Self::Unknown(code)),
                    }
                }

                /// Get the corresponding error code to the error
                pub fn code(&self) -> i16 {
                    match *self {
                        $name::Unknown(code) => code,
                        $(
                            $name::[<$kind:camel>] => $code,
                        )*
                    }
                }

                /// Check if is it a retriable error.
                ///
                /// Whether an error is retriable or not is defined by the Kafka protocol.
                pub fn is_retriable(&self) -> bool {
                    match *self {
                        // Client-side unknown mark as non-retriable
                        $name::Unknown(_) => false,

                        $(
                            $name::[<$kind:camel>] => $retriable,
                        )*
                    }
                }

                fn try_as_str(&self) -> Result<&'static str, i16> {
                    match *self {
                        $name::Unknown(code) => Err(code),
                        $(
                            $name::[<$kind:camel>] => Ok(stringify!([<$kind:camel>])),
                        )*
                    }
                }
            }
        }
    }
}

define_errors! { ResponseError,
    (UNKNOWN_SERVER_ERROR,                  -1, false, "The server experienced an unexpected error when processing the request."),
    // (NONE,                                  0, false, "No error"),
    (OFFSET_OUT_OF_RANGE,                   1, false, "The requested offset is not within the range of offsets maintained by the server."),
    (CORRUPT_MESSAGE,                       2, true, "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."),
    (UNKNOWN_TOPIC_OR_PARTITION,            3, true, "This server does not host this topic-partition."),
    (INVALID_FETCH_SIZE,                    4, false, "The requested fetch size is invalid."),
    (LEADER_NOT_AVAILABLE,                  5, true, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    (NOT_LEADER_OR_FOLLOWER,                6, true, "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."),
    (REQUEST_TIMED_OUT,                     7, true, "The request timed out."),
    (BROKER_NOT_AVAILABLE,                  8, false, "The broker is not available."),
    (REPLICA_NOT_AVAILABLE,                 9, true,  "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."),
    (MESSAGE_TOO_LARGE,                     10, false, "The request included a message larger than the max message size the server will accept."),
    (STALE_CONTROLLER_EPOCH,                11, false, "The controller moved to another broker."),
    (OFFSET_METADATA_TOO_LARGE,             12, false, "The metadata field of the offset request was too large."),
    (NETWORK_EXCEPTION,                     13, true, "The server disconnected before a response was received."),
    (COORDINATOR_LOAD_IN_PROGRESS,          14, true, "The coordinator is loading and hence can't process requests."),
    (COORDINATOR_NOT_AVAILABLE,             15, true, "The coordinator is not available."),
    (NOT_COORDINATOR,                       16, true, "This is not the correct coordinator."),
    (INVALID_TOPIC_EXCEPTION,               17, false, "The request attempted to perform an operation on an invalid topic."),
    (RECORD_LIST_TOO_LARGE,                 18, false, "The request included message batch larger than the configured segment size on the server."),
    (NOT_ENOUGH_REPLICAS,                   19, true, "Messages are rejected since there are fewer in-sync replicas than required."),
    (NOT_ENOUGH_REPLICAS_AFTER_APPEND,      20, true, "Messages are written to the log, but to fewer in-sync replicas than required."),
    (INVALID_REQUIRED_ACKS,                 21, false, "Produce request specified an invalid value for required acks."),
    (ILLEGAL_GENERATION,                    22, false, "Specified group generation id is not valid."),
    (INCONSISTENT_GROUP_PROTOCOL,           23, false, "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."),
    (INVALID_GROUP_ID,                      24, false, "The configured groupId is invalid."),
    (UNKNOWN_MEMBER_ID,                     25, false, "The coordinator is not aware of this member."),
    (INVALID_SESSION_TIMEOUT,               26, false, "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."),
    (REBALANCE_IN_PROGRESS,                 27, false, "The group is rebalancing, so a rejoin is needed."),
    (INVALID_COMMIT_OFFSET_SIZE,            28, false, "The committing offset data size is not valid."),
    (TOPIC_AUTHORIZATION_FAILED,            29, false, "Topic authorization failed."),
    (GROUP_AUTHORIZATION_FAILED,            30, false, "Group authorization failed."),
    (CLUSTER_AUTHORIZATION_FAILED,          31, false, "Cluster authorization failed."),
    (INVALID_TIMESTAMP,                     32, false, "The timestamp of the message is out of acceptable range."),
    (UNSUPPORTED_SASL_MECHANISM,            33, false, "The broker does not support the requested SASL mechanism."),
    (ILLEGAL_SASL_STATE,                    34, false, "Request is not valid given the current SASL state."),
    (UNSUPPORTED_VERSION,                   35, false, "The version of API is not supported."),
    (TOPIC_ALREADY_EXISTS,                  36, false, "Topic with this name already exists."),
    (INVALID_PARTITIONS,                    37, false, "Number of partitions is below 1."),
    (INVALID_REPLICATION_FACTOR,            38, false, "Replication factor is below 1 or larger than the number of available brokers."),
    (INVALID_REPLICA_ASSIGNMENT,            39, false, "Replica assignment is invalid."),
    (INVALID_CONFIG,                        40, false, "Configuration is invalid."),
    (NOT_CONTROLLER,                        41, true, "This is not the correct controller for this cluster."),
    (INVALID_REQUEST,                       42, false, "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."),
    (UNSUPPORTED_FOR_MESSAGE_FORMAT,        43, false, "The message format version on the broker does not support the request."),
    (POLICY_VIOLATION,                      44, false, "Request parameters do not satisfy the configured policy."),
    (OUT_OF_ORDER_SEQUENCE_NUMBER,          45, false, "The broker received an out of order sequence number."),
    (DUPLICATE_SEQUENCE_NUMBER,             46, false, "The broker received a duplicate sequence number."),
    (INVALID_PRODUCER_EPOCH,                47, false, "Producer attempted to produce with an old epoch."),
    (INVALID_TXN_STATE,                     48, false, "The producer attempted a transactional operation in an invalid state."),
    (INVALID_PRODUCER_ID_MAPPING,           49, false, "The producer attempted to use a producer id which is not currently assigned to its transactional id."),
    (INVALID_TRANSACTION_TIMEOUT,           50, false, "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."),
    (CONCURRENT_TRANSACTIONS,               51, false, "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."),
    (TRANSACTION_COORDINATOR_FENCED,        52, false, "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."),
    (TRANSACTIONAL_ID_AUTHORIZATION_FAILED, 53, false, "Transactional Id authorization failed."),
    (SECURITY_DISABLED,                     54, false, "Security features are disabled."),
    (OPERATION_NOT_ATTEMPTED,               55, false, "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest."),
    (KAFKA_STORAGE_ERROR,                   56, true, "Disk error when trying to access log file on the disk."),
    (LOG_DIR_NOT_FOUND,                     57, false, "The user-specified log directory is not found in the broker config."),
    (SASL_AUTHENTICATION_FAILED,            58, false, "SASL Authentication failed."),
    (UNKNOWN_PRODUCER_ID,                   59, false, "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."),
    (REASSIGNMENT_IN_PROGRESS,              60, false, "A partition reassignment is in progress."),
    (DELEGATION_TOKEN_AUTH_DISABLED,        61, false, "Delegation Token feature is not enabled."),
    (DELEGATION_TOKEN_NOT_FOUND,            62, false, "Delegation Token is not found on server."),
    (DELEGATION_TOKEN_OWNER_MISMATCH,       63, false, "Specified Principal is not valid Owner/Renewer."),
    (DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,  64, false, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."),
    (DELEGATION_TOKEN_AUTHORIZATION_FAILED, 65, false, "Delegation Token authorization failed."),
    (DELEGATION_TOKEN_EXPIRED,              66, false, "Delegation Token is expired."),
    (INVALID_PRINCIPAL_TYPE,                67, false, "Supplied principalType is not supported."),
    (NON_EMPTY_GROUP,                       68, false, "The group is not empty."),
    (GROUP_ID_NOT_FOUND,                    69, false, "The group id does not exist."),
    (FETCH_SESSION_ID_NOT_FOUND,            70, true, "The fetch session ID was not found."),
    (INVALID_FETCH_SESSION_EPOCH,           71, true, "The fetch session epoch is invalid."),
    (LISTENER_NOT_FOUND,                    72, true, "There is no listener on the leader broker that matches the listener on which metadata request was processed."),
    (TOPIC_DELETION_DISABLED,               73, false, "Topic deletion is disabled."),
    (FENCED_LEADER_EPOCH,                   74, true, "The leader epoch in the request is older than the epoch on the broker."),
    (UNKNOWN_LEADER_EPOCH,                  75, true, "The leader epoch in the request is newer than the epoch on the broker."),
    (UNSUPPORTED_COMPRESSION_TYPE,          76, false, "The requesting client does not support the compression type of given partition."),
    (STALE_BROKER_EPOCH,                    77, false, "Broker epoch has changed."),
    (OFFSET_NOT_AVAILABLE,                  78, true, "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."),
    (MEMBER_ID_REQUIRED,                    79, false, "The group member needs to have a valid member id before actually entering a consumer group."),
    (PREFERRED_LEADER_NOT_AVAILABLE,        80, true, "The preferred leader was not available."),
    (GROUP_MAX_SIZE_REACHED,                81, false, "The consumer group has reached its max size."),
    (FENCED_INSTANCE_ID,                    82, false, "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."),
    (ELIGIBLE_LEADERS_NOT_AVAILABLE,        83, true, "Eligible topic partition leaders are not available."),
    (ELECTION_NOT_NEEDED,                   84, true, "Leader election not needed for topic partition."),
    (NO_REASSIGNMENT_IN_PROGRESS,           85, false, "No partition reassignment is in progress."),
    (GROUP_SUBSCRIBED_TO_TOPIC,             86, false, "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."),
    (INVALID_RECORD,                        87, false, "This record has failed the validation on broker and hence will be rejected."),
    (UNSTABLE_OFFSET_COMMIT,                88, true, "There are unstable offsets that need to be cleared."),
    (THROTTLING_QUOTA_EXCEEDED,             89, true, "The throttling quota has been exceeded."),
    (PRODUCER_FENCED,                       90, false, "There is a newer producer with the same transactionalId which fences the current one."),
    (RESOURCE_NOT_FOUND,                    91, false, "A request illegally referred to a resource that does not exist."),
    (DUPLICATE_RESOURCE,                    92, false, "A request illegally referred to the same resource twice."),
    (UNACCEPTABLE_CREDENTIAL,               93, false, "Requested credential would not meet criteria for acceptability."),
    (INCONSISTENT_VOTER_SET,                94, false, "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters"),
    (INVALID_UPDATE_VERSION,                95, false, "The given update version was invalid."),
    (FEATURE_UPDATE_FAILED,                 96, false, "Unable to update finalized features due to an unexpected server error."),
    (PRINCIPAL_DESERIALIZATION_FAILURE,     97, false, "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup."),
    (SNAPSHOT_NOT_FOUND,                    98, false, "Requested snapshot was not found"),
    (POSITION_OUT_OF_RANGE,                 99, false, "Requested position is not greater than or equal to zero, and less than the size of the snapshot."),
    (UNKNOWN_TOPIC_ID,                      100, true, "This server does not host this topic ID."),
    (DUPLICATE_BROKER_REGISTRATION,         101, false, "This broker ID is already in use."),
    (BROKER_ID_NOT_REGISTERED,              102, false, "The given broker ID was not registered."),
    (INCONSISTENT_TOPIC_ID,                 103, true, "The log's topic ID did not match the topic ID in the request"),
    (INCONSISTENT_CLUSTER_ID,               104, false, "The clusterId in the request does not match that found on the server"),
    (TRANSACTIONAL_ID_NOT_FOUND,            105, false, "The transactionalId could not be found"),
    (FETCH_SESSION_TOPIC_ID_ERROR,          106, true, "The fetch session encountered inconsistent topic ID usage"),
    // since Kafka 3.3.0
    (INELIGIBLE_REPLICA,                    107, false, "The new ISR contains at least one ineligible replica."),
    (NEW_LEADER_ELECTED,                    108, false, "The AlterPartition request successfully updated the partition state but the leader has changed."),
    // since Kafka 3.5.0
    (OFFSET_MOVED_TO_TIERED_STORAGE,        109, false, "The requested offset is moved to tiered storage."),
    (FENCED_MEMBER_EPOCH,                   110, false, "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin."),
    (UNRELEASED_INSTANCE_ID,                111, false, "The instance ID is still used by another member in the consumer group. That member must leave first."),
    (UNSUPPORTED_ASSIGNOR,                  112, false, "The assignor or its version range is not supported by the consumer group."),
    // since Kafka 3.6.0
    (STALE_MEMBER_EPOCH,                    113, false, "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API."),
    // since Kafka 3.7.0
    (MISMATCHED_ENDPOINT_TYPE,              114, false, "The request was sent to an endpoint of the wrong type."),
    (UNSUPPORTED_ENDPOINT_TYPE,             115, false, "This endpoint type is not supported yet."),
    (UNKNOWN_CONTROLLER_ID,                 116, false, "This controller ID is not known."),
    (UNKNOWN_SUBSCRIPTION_ID,               117, false, "Client sent a push telemetry request with an invalid or outdated subscription ID."),
    (TELEMETRY_TOO_LARGE,                   118, false, "Client sent a push telemetry request larger than the maximum size the broker will accept."),
    (INVALID_REGISTRATION,                  119, false, "The controller has considered the broker registration to be invalid."),
    // since Kafka 3.8.0
    (TRANSACTION_ABORTABLE,                 120, false, "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID."),
    // since Kafka 3.9.0
    (INVALID_RECORD_STATE,                  121, false, "The record state is invalid. The acknowledgement of delivery could not be completed."),
    (SHARE_SESSION_NOT_FOUND,               122, true,  "The share session was not found."),
    (INVALID_SHARE_SESSION_EPOCH,           123, true,  "The share session epoch is invalid."),
    (FENCED_STATE_EPOCH,                    124, false, "The share coordinator rejected the request because the share-group state epoch did not match."),
    (INVALID_VOTER_KEY,                     125, false, "The voter key doesn't match the receiving replica's key."),
    (DUPLICATE_VOTER,                       126, false, "The voter is already part of the set of voters."),
    (VOTER_NOT_FOUND,                       127, false, "The voter is not part of the set of voters."),
    // since Kafka 4.0.0
    (INVALID_REGULAR_EXPRESSION,            128, false, "The regular expression is not valid."),
    (REBOOTSTRAP_REQUIRED,                  129, false, "Client metadata is stale, client should rebootstrap to obtain new metadata."),
    // since Kafka 4.1.0
    (STREAMS_INVALID_TOPOLOGY,              130, false, "The supplied topology is invalid."),
    (STREAMS_INVALID_TOPOLOGY_EPOCH,        131, false, "The supplied topology epoch is invalid."),
    (STREAMS_TOPOLOGY_FENCED,               132, false, "The supplied topology epoch is outdated."),
    (SHARE_SESSION_LIMIT_REACHED,           133, true, "The limit of share sessions has been reached."),
}
