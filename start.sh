java -Djava.net.preferIPv4Stack=true -Djava.library.path="./sigar-native-libs" -XX:+UseG1GC -XX:-MaxFDLimit -XX:+HeapDumpOnOutOfMemoryError -Xmx6g -Xms3g  -cp :./target/httpasync-1.0.0-jar-with-dependencies.jar com.example.Bootstrap