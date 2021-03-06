-module(mqtt_msg_test).
-include_lib("eunit/include/eunit.hrl").

basic_connect_test() ->
    ClientId = <<"test">>,
    Con = mqtt_msg:connect(#{client_id => ClientId}),
    {{connect, Map}, <<>>} = mqtt_msg:parse(Con),
    StringMap = maps:get(string_map, Map),
    true = maps:is_key(client_id, StringMap),
    true = maps:is_key(will_topic, StringMap),
    true = maps:is_key(will_message, StringMap),
    true = maps:is_key(user_name, StringMap),
    true = maps:is_key(password, StringMap),
    ClientId = maps:get(client_id, StringMap).

will_connect_test() ->
    ClientId = <<"test">>,
    WillTopic = <<"dc/will">>,
    WillMessage = <<"I am out">>,
    KeepAlive = 300,
    Con = mqtt_msg:connect(#{client_id => ClientId, 
			     will_topic => WillTopic,
			     will_message => WillMessage,
			     keep_alive => KeepAlive
			    }),
    {{connect, Map}, <<>>} = mqtt_msg:parse(Con),
    StringMap = maps:get(string_map, Map),
    true = maps:is_key(client_id, StringMap),
    true = maps:is_key(will_topic, StringMap),
    true = maps:is_key(will_message, StringMap),
    true = maps:is_key(user_name, StringMap),
    true = maps:is_key(password, StringMap),
    ClientId = maps:get(client_id, StringMap),
    WillMessage = maps:get(will_message, StringMap),
    WillTopic = maps:get(will_topic, StringMap),
    KeepAlive = maps:get(keep_alive, Map),
    ok.


connack_test() ->
    ConnAck = mqtt_msg:connack(false, 0),
    {{connack, _}, <<>>} = mqtt_msg:parse(ConnAck).

publish1_test() ->
    Publish1 = mqtt_msg:publish(<<"test">>,<<"one">>,4,0),
    {{publish, Map1}, <<>>} = mqtt_msg:parse(Publish1),
    -1 = maps:get(packet_id, Map1, 0).

publish2_test() ->
    Publish2 = mqtt_msg:publish(<<"test">>,<<"one">>,4,1),
    {{publish, Map2}, <<>>} = mqtt_msg:parse(Publish2),
    4 = maps:get(packet_id, Map2, 0).

puback_test() ->
    PubAck = mqtt_msg:puback(4),
    {{puback, _}, <<>>} = mqtt_msg:parse(PubAck).

pubrec_test() ->
    PubRec = mqtt_msg:pubrec(4),
    {{pubrec, _}, <<>>} = mqtt_msg:parse(PubRec).

pubrel_test() ->
    PubRel = mqtt_msg:pubrel(4),
    {{pubrel, _}, <<>>} = mqtt_msg:parse(PubRel).

pubcomp_test() ->
    PubComp = mqtt_msg:pubcomp(4),
    {{pubcomp, _}, <<>>} = mqtt_msg:parse(PubComp).

subscribe_test() ->
    Subscribe = mqtt_msg:subscribe(23,[{<<"topic1">>,2},{<<"topic2">>,0}]),
    {{subscribe, _}, <<>>} = mqtt_msg:parse(Subscribe).

suback_test() ->
    Suback = mqtt_msg:suback(23, [1,0]),
    {{suback, _}, <<>>} = mqtt_msg:parse(Suback).

unsubscribe_test() ->
    Unsubscribe = mqtt_msg:unsubscribe(13, [<<"topic1">>, <<"topic2">>]),
    {{unsubscribe, _}, <<>>} = mqtt_msg:parse(Unsubscribe).

unsuback_test() ->
    Unsuback = mqtt_msg:unsuback(13),
    {{unsuback, _}, <<>>} = mqtt_msg:parse(Unsuback).

pingreq_test() ->
    Pingreq = mqtt_msg:pingreq(),
    {{pingreq, _}, <<>>} = mqtt_msg:parse(Pingreq).

pingresp_test() ->
    Pingresp = mqtt_msg:pingresp(),
    {{pingresp, _}, <<>>} = mqtt_msg:parse(Pingresp).

disconnect_test() ->
    Disconnect = mqtt_msg:disconnect(),
    {{disconnect, _}, <<>>} = mqtt_msg:parse(Disconnect).


