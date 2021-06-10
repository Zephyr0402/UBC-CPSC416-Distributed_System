# java -jar ./GraderA4-SmokeTest.jar -n 2 -r client1:AQIDBA==:5:0 -r client1:AQIDBA==:5:1 -t ../trace_output.log &> result.log
# java -jar GraderA4-SmokeTest.jar --num-workers 2 --run-info client1:AQIDBA==:7:0 --run-info client1:BQYHCA==:5:0 --run-info client2:AgICAg==:5:0 --run-info client2:AgICAg==:7:0 --trace-file ../trace_output.log &> result.log
java -jar GraderA4-SmokeTest.jar --num-workers 4 \
--run-info client1:AQIDBA==:5:0 \
--run-info client1:AQIDBA==:5:1 \
--run-info client1:AQIDBA==:6:0 \
--run-info client1:AQIDBA==:4:0 \
--run-info client2:AgICAg==:4:0 \
--run-info client2:AgICAg==:7:0 \
--trace-file ../trace_output.log &> result1.log