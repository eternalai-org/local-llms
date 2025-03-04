source local_llms/bin/activate
if [ "$1" == "start" ]; then
    local-llms start --hash bafkreih572a2uvcvyzpvdhvbgrplf6jdfgcvstb5ahvk7nxmlh7r2wfzve
else
    local-llms stop
fi