//! Error kinds for Kafka error codes.

/// A trait providing tools for parsing Kafka response error code.
pub trait ParseResponseErrorCode {
    /// Convert from an i16 error code to `Option<()>`, if is ok, returns `Some(())`.
    ///
    /// Convert self into an Option<()>, consuming self, and discarding the error code, if any.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use kafka_protocol::error::ParseResponseErrorCode;
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
    /// use kafka_protocol::error::{
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
    /// use kafka_protocol::error::ParseResponseErrorCode;
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
    /// use kafka_protocol::error::ParseResponseErrorCode;
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
                $kind,
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
            pub const fn try_from_code(code: i16) -> Option<Self> {
                match code {
                    0 => None,
                    $(
                        $code => Some($name::$kind),
                    )*
                    _ => Some(Self::Unknown(code)),
                }
            }

            /// Get the corresponding error code to the error
            pub const fn code(&self) -> i16 {
                match *self {
                    $name::Unknown(code) => code,
                    $(
                        $name::$kind => $code,
                    )*
                }
            }

            /// Check if is it a retriable error.
            ///
            /// Whether an error is retriable or not is defined by the Kafka protocol.
            pub const fn is_retriable(&self) -> bool {
                match *self {
                    // Client-side unknown mark as non-retriable
                    $name::Unknown(_) => false,

                    $(
                        $name::$kind => $retriable,
                    )*
                }
            }

            fn try_as_str(&self) -> Result<&'static str, i16> {
                match *self {
                    $name::Unknown(code) => Err(code),
                    $(
                        $name::$kind => Ok(stringify!($kind)),
                    )*
                }
            }
        }
    }
}

define_errors! { ResponseError,
    (UnknownServerError,                  -1, false, "The server experienced an unexpected error when processing the request."),
    // (None,                                  0, false, "No error"),
    (OffsetOutOfRange,                   1, false, "The requested offset is not within the range of offsets maintained by the server."),
    (CorruptMessage,                       2, true, "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."),
    (UnknownTopicOrPartition,            3, true, "This server does not host this topic-partition."),
    (InvalidFetchSize,                    4, false, "The requested fetch size is invalid."),
    (LeaderNotAvailable,                  5, true, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    (NotLeaderOrFollower,                6, true, "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."),
    (RequestTimedOut,                     7, true, "The request timed out."),
    (BrokerNotAvailable,                  8, false, "The broker is not available."),
    (ReplicaNotAvailable,                 9, true,  "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."),
    (MessageTooLarge,                     10, false, "The request included a message larger than the max message size the server will accept."),
    (StaleControllerEpoch,                11, false, "The controller moved to another broker."),
    (OffsetMetadataTooLarge,             12, false, "The metadata field of the offset request was too large."),
    (NetworkException,                     13, true, "The server disconnected before a response was received."),
    (CoordinatorLoadInProgress,          14, true, "The coordinator is loading and hence can't process requests."),
    (CoordinatorNotAvailable,             15, true, "The coordinator is not available."),
    (NotCoordinator,                       16, true, "This is not the correct coordinator."),
    (InvalidTopicException,               17, false, "The request attempted to perform an operation on an invalid topic."),
    (RecordListTooLarge,                 18, false, "The request included message batch larger than the configured segment size on the server."),
    (NotEnoughReplicas,                   19, true, "Messages are rejected since there are fewer in-sync replicas than required."),
    (NotEnoughReplicasAfterAppend,      20, true, "Messages are written to the log, but to fewer in-sync replicas than required."),
    (InvalidRequiredAcks,                 21, false, "Produce request specified an invalid value for required acks."),
    (IllegalGeneration,                    22, false, "Specified group generation id is not valid."),
    (InconsistentGroupProtocol,           23, false, "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."),
    (InvalidGroupId,                      24, false, "The configured groupId is invalid."),
    (UnknownMemberId,                     25, false, "The coordinator is not aware of this member."),
    (InvalidSessionTimeout,               26, false, "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."),
    (RebalanceInProgress,                 27, false, "The group is rebalancing, so a rejoin is needed."),
    (InvalidCommitOffsetSize,            28, false, "The committing offset data size is not valid."),
    (TopicAuthorizationFailed,            29, false, "Topic authorization failed."),
    (GroupAuthorizationFailed,            30, false, "Group authorization failed."),
    (ClusterAuthorizationFailed,          31, false, "Cluster authorization failed."),
    (InvalidTimestamp,                     32, false, "The timestamp of the message is out of acceptable range."),
    (UnsupportedSaslMechanism,            33, false, "The broker does not support the requested SASL mechanism."),
    (IllegalSaslState,                    34, false, "Request is not valid given the current SASL state."),
    (UnsupportedVersion,                   35, false, "The version of API is not supported."),
    (TopicAlreadyExists,                  36, false, "Topic with this name already exists."),
    (InvalidPartitions,                    37, false, "Number of partitions is below 1."),
    (InvalidReplicationFactor,            38, false, "Replication factor is below 1 or larger than the number of available brokers."),
    (InvalidReplicaAssignment,            39, false, "Replica assignment is invalid."),
    (InvalidConfig,                        40, false, "Configuration is invalid."),
    (NotController,                        41, true, "This is not the correct controller for this cluster."),
    (InvalidRequest,                       42, false, "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."),
    (UnsupportedForMessageFormat,        43, false, "The message format version on the broker does not support the request."),
    (PolicyViolation,                      44, false, "Request parameters do not satisfy the configured policy."),
    (OutOfOrderSequenceNumber,          45, false, "The broker received an out of order sequence number."),
    (DuplicateSequenceNumber,             46, false, "The broker received a duplicate sequence number."),
    (InvalidProducerEpoch,                47, false, "Producer attempted to produce with an old epoch."),
    (InvalidTxnState,                     48, false, "The producer attempted a transactional operation in an invalid state."),
    (InvalidProducerIdMapping,           49, false, "The producer attempted to use a producer id which is not currently assigned to its transactional id."),
    (InvalidTransactionTimeout,           50, false, "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."),
    (ConcurrentTransactions,               51, false, "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."),
    (TransactionCoordinatorFenced,        52, false, "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."),
    (TransactionalIdAuthorizationFailed, 53, false, "Transactional Id authorization failed."),
    (SecurityDisabled,                     54, false, "Security features are disabled."),
    (OperationNotAttempted,               55, false, "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest."),
    (KafkaStorageError,                   56, true, "Disk error when trying to access log file on the disk."),
    (LogDirNotFound,                     57, false, "The user-specified log directory is not found in the broker config."),
    (SaslAuthenticationFailed,            58, false, "SASL Authentication failed."),
    (UnknownProducerId,                   59, false, "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."),
    (ReassignmentInProgress,              60, false, "A partition reassignment is in progress."),
    (DelegationTokenAuthDisabled,        61, false, "Delegation Token feature is not enabled."),
    (DelegationTokenNotFound,            62, false, "Delegation Token is not found on server."),
    (DelegationTokenOwnerMismatch,       63, false, "Specified Principal is not valid Owner/Renewer."),
    (DelegationTokenRequestNotAllowed,  64, false, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."),
    (DelegationTokenAuthorizationFailed, 65, false, "Delegation Token authorization failed."),
    (DelegationTokenExpired,              66, false, "Delegation Token is expired."),
    (InvalidPrincipalType,                67, false, "Supplied principalType is not supported."),
    (NonEmptyGroup,                       68, false, "The group is not empty."),
    (GroupIdNotFound,                    69, false, "The group id does not exist."),
    (FetchSessionIdNotFound,            70, true, "The fetch session ID was not found."),
    (InvalidFetchSessionEpoch,           71, true, "The fetch session epoch is invalid."),
    (ListenerNotFound,                    72, true, "There is no listener on the leader broker that matches the listener on which metadata request was processed."),
    (TopicDeletionDisabled,               73, false, "Topic deletion is disabled."),
    (FencedLeaderEpoch,                   74, true, "The leader epoch in the request is older than the epoch on the broker."),
    (UnknownLeaderEpoch,                  75, true, "The leader epoch in the request is newer than the epoch on the broker."),
    (UnsupportedCompressionType,          76, false, "The requesting client does not support the compression type of given partition."),
    (StaleBrokerEpoch,                    77, false, "Broker epoch has changed."),
    (OffsetNotAvailable,                  78, true, "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."),
    (MemberIdRequired,                    79, false, "The group member needs to have a valid member id before actually entering a consumer group."),
    (PreferredLeaderNotAvailable,        80, true, "The preferred leader was not available."),
    (GroupMaxSizeReached,                81, false, "The consumer group has reached its max size."),
    (FencedInstanceId,                    82, false, "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."),
    (EligibleLeadersNotAvailable,        83, true, "Eligible topic partition leaders are not available."),
    (ElectionNotNeeded,                   84, true, "Leader election not needed for topic partition."),
    (NoReassignmentInProgress,           85, false, "No partition reassignment is in progress."),
    (GroupSubscribedToTopic,             86, false, "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."),
    (InvalidRecord,                        87, false, "This record has failed the validation on broker and hence will be rejected."),
    (UnstableOffsetCommit,                88, true, "There are unstable offsets that need to be cleared."),
    (ThrottlingQuotaExceeded,             89, true, "The throttling quota has been exceeded."),
    (ProducerFenced,                       90, false, "There is a newer producer with the same transactionalId which fences the current one."),
    (ResourceNotFound,                    91, false, "A request illegally referred to a resource that does not exist."),
    (DuplicateResource,                    92, false, "A request illegally referred to the same resource twice."),
    (UnacceptableCredential,               93, false, "Requested credential would not meet criteria for acceptability."),
    (InconsistentVoterSet,                94, false, "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters"),
    (InvalidUpdateVersion,                95, false, "The given update version was invalid."),
    (FeatureUpdateFailed,                 96, false, "Unable to update finalized features due to an unexpected server error."),
    (PrincipalDeserializationFailure,     97, false, "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup."),
    (SnapshotNotFound,                    98, false, "Requested snapshot was not found"),
    (PositionOutOfRange,                 99, false, "Requested position is not greater than or equal to zero, and less than the size of the snapshot."),
    (UnknownTopicId,                      100, true, "This server does not host this topic ID."),
    (DuplicateBrokerRegistration,         101, false, "This broker ID is already in use."),
    (BrokerIdNotRegistered,              102, false, "The given broker ID was not registered."),
    (InconsistentTopicId,                 103, true, "The log's topic ID did not match the topic ID in the request"),
    (InconsistentClusterId,               104, false, "The clusterId in the request does not match that found on the server"),
    (TransactionalIdNotFound,            105, false, "The transactionalId could not be found"),
    (FetchSessionTopicIdError,          106, true, "The fetch session encountered inconsistent topic ID usage"),
    // since Kafka 3.3.0
    (IneligibleReplica,                    107, false, "The new ISR contains at least one ineligible replica."),
    (NewLeaderElected,                    108, false, "The AlterPartition request successfully updated the partition state but the leader has changed."),
    // since Kafka 3.5.0
    (OffsetMovedToTieredStorage,        109, false, "The requested offset is moved to tiered storage."),
    (FencedMemberEpoch,                   110, false, "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin."),
    (UnreleasedInstanceId,                111, false, "The instance ID is still used by another member in the consumer group. That member must leave first."),
    (UnsupportedAssignor,                  112, false, "The assignor or its version range is not supported by the consumer group."),
    // since Kafka 3.6.0
    (StaleMemberEpoch,                    113, false, "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API."),
    // since Kafka 3.7.0
    (MismatchedEndpointType,              114, false, "The request was sent to an endpoint of the wrong type."),
    (UnsupportedEndpointType,             115, false, "This endpoint type is not supported yet."),
    (UnknownControllerId,                 116, false, "This controller ID is not known."),
    (UnknownSubscriptionId,               117, false, "Client sent a push telemetry request with an invalid or outdated subscription ID."),
    (TelemetryTooLarge,                   118, false, "Client sent a push telemetry request larger than the maximum size the broker will accept."),
    (InvalidRegistration,                  119, false, "The controller has considered the broker registration to be invalid."),
    // since Kafka 3.8.0
    (TransactionAbortable,                 120, false, "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID."),
    // since Kafka 3.9.0
    (InvalidRecordState,                  121, false, "The record state is invalid. The acknowledgement of delivery could not be completed."),
    (ShareSessionNotFound,               122, true,  "The share session was not found."),
    (InvalidShareSessionEpoch,           123, true,  "The share session epoch is invalid."),
    (FencedStateEpoch,                    124, false, "The share coordinator rejected the request because the share-group state epoch did not match."),
    (InvalidVoterKey,                     125, false, "The voter key doesn't match the receiving replica's key."),
    (DuplicateVoter,                       126, false, "The voter is already part of the set of voters."),
    (VoterNotFound,                       127, false, "The voter is not part of the set of voters."),
    // since Kafka 4.0.0
    (InvalidRegularExpression,            128, false, "The regular expression is not valid."),
    (RebootstrapRequired,                  129, false, "Client metadata is stale, client should rebootstrap to obtain new metadata."),
    // since Kafka 4.1.0
    (StreamsInvalidTopology,              130, false, "The supplied topology is invalid."),
    (StreamsInvalidTopologyEpoch,        131, false, "The supplied topology epoch is invalid."),
    (StreamsTopologyFenced,               132, false, "The supplied topology epoch is outdated."),
    (ShareSessionLimitReached,           133, true, "The limit of share sessions has been reached."),
}
