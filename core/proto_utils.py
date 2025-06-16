# import json
# from datetime import datetime
# from typing import Dict, Any, Union
# from proto.message_pb2 import Publication, Subscription, Condition, BrokerMessage
#
# def serialize_publication(publication: Dict[str, Any]) -> bytes:
#     """Serialize a publication dictionary to Protocol Buffers format."""
#     proto_publication = Publication(
#         type=publication['type'],
#         value=publication['value'],
#         timestamp=publication['timestamp']
#     )
#     return proto_publication.SerializeToString()
#
# def deserialize_publication(data: bytes) -> Dict[str, Any]:
#     """Deserialize Protocol Buffers data to a publication dictionary."""
#     proto_publication = Publication()
#     proto_publication.ParseFromString(data)
#     return {
#         'type': proto_publication.type,
#         'value': proto_publication.value,
#         'timestamp': proto_publication.timestamp
#     }
#
# def serialize_subscription(subscription: Dict[str, Any]) -> bytes:
#     """Serialize a subscription dictionary to Protocol Buffers format."""
#     conditions = []
#     for cond in subscription['conditions']:
#         proto_condition = Condition(
#             field=cond['field'],
#             operator=cond['operator'],
#             value=cond['value']
#         )
#         conditions.append(proto_condition)
#
#     proto_subscription = Subscription(
#         conditions=conditions,
#         window_size=subscription.get('window_size', 0)
#     )
#     return proto_subscription.SerializeToString()
#
# def deserialize_subscription(data: bytes) -> Dict[str, Any]:
#     """Deserialize Protocol Buffers data to a subscription dictionary."""
#     proto_subscription = Subscription()
#     proto_subscription.ParseFromString(data)
#
#     conditions = []
#     for cond in proto_subscription.conditions:
#         conditions.append({
#             'field': cond.field,
#             'operator': cond.operator,
#             'value': cond.value
#         })
#
#     return {
#         'conditions': conditions,
#         'window_size': proto_subscription.window_size
#     }
#
# def create_broker_message(content: Union[Dict[str, Any], Dict[str, Any]],
#                          message_type: BrokerMessage.MessageType) -> bytes:
#     """Create and serialize a broker message."""
#     broker_message = BrokerMessage(type=message_type)
#
#     if message_type == BrokerMessage.PUBLICATION:
#         broker_message.publication.CopyFrom(
#             Publication(
#                 type=content['type'],
#                 value=content['value'],
#                 timestamp=content['timestamp']
#             )
#         )
#     else:  # SUBSCRIPTION
#         conditions = []
#         for cond in content['conditions']:
#             conditions.append(
#                 Condition(
#                     field=cond['field'],
#                     operator=cond['operator'],
#                     value=cond['value']
#                 )
#             )
#         broker_message.subscription.CopyFrom(
#             Subscription(
#                 conditions=conditions,
#                 window_size=content.get('window_size', 0)
#             )
#         )
#
#     return broker_message.SerializeToString()
#
# def parse_broker_message(data: bytes) -> tuple[BrokerMessage.MessageType, Dict[str, Any]]:
#     """Parse a broker message and return its type and content."""
#     broker_message = BrokerMessage()
#     broker_message.ParseFromString(data)
#
#     if broker_message.type == BrokerMessage.PUBLICATION:
#         pub = broker_message.publication
#         content = {
#             'type': pub.type,
#             'value': pub.value,
#             'timestamp': pub.timestamp
#         }
#     else:  # SUBSCRIPTION
#         sub = broker_message.subscription
#         conditions = []
#         for cond in sub.conditions:
#             conditions.append({
#                 'field': cond.field,
#                 'operator': cond.operator,
#                 'value': cond.value
#             })
#         content = {
#             'conditions': conditions,
#             'window_size': sub.window_size
#         }
#
#     return broker_message.type, content