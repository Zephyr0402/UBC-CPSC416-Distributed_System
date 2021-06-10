rm -f ./grade.log
sh ./gradera6-smoketest -c client1 -c client2 -s storage1 storage2 storage3 \
-r client1:put:1:key3 \
-r client1:get:3:key1 \
-r client1:put:1:key4 \
-r client2:put:1:key1 \
-r client2:get:1:key1 \
-r client1:put:1:key2 \
-r client2:get:1:key2 \
-r client2:get:1:key3 \
-t ../trace_output.log &> grade.log



#-r client1:put:200:key3 \
#-r client1:get:200:key3 \
#-r client1:put:200:key2 \
#-r client1:get:200:key2 \