#!/bin/bash

# Get the current directory
#sudo lsof -ti:8765,1883 | xargs kill
current_dir=$(pwd)

# Command to launch Mosquitto
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; /usr/local/opt/mosquitto/sbin/mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf"
end tell
END

sleep 1
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; source 38test/bin/activate; python3 hostname_resolver.py config.yaml"
end tell
END
# Command to run subscriber_handler.py
#osascript <<END
#tell application "Terminal"
#    do script "cd '$current_dir'; python3 subscriber_handler.py"
#end tell
#END
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 JSONtest.py"
end tell
END

# Command to run test.py
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 test.py"
end tell
END







