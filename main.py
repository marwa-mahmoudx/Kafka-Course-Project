#Main

import subprocess
import time

def start_process(script_name):
    #Helper function to start a process
    return subprocess.Popen(['python', script_name])

def main():
    # Start the Flask server
    print("Starting Flask server...")
    flask_server = start_process('server.py')
    time.sleep(3)  # Wait for the server to start

    
    #print("Starting Kafka admin script...")
    #admin_script = start_process('admin.py')
    #time.sleep(3)  # Wait for the admin script to execute
    
    
    # Start the WebSocket server
    print("Starting WebSocket server...")
    websocket_server = start_process('websocket-server.py')
    time.sleep(3)  # Wait for the WebSocket server to start

    # Start the Kafka producers and consumers
    print("Starting Kafka producer...")
    producer_script = start_process('producer.py')
    
    print("Starting Kafka consumer group 1...")
    consumer_gp1_script = start_process('consumer-gp1.py')

    print("Starting Kafka consumer group 2...")
    consumer_gp2_script = start_process('consumer-gp2.py')


    try:
        # Keep the script running and manage subprocesses
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Terminating processes...")
        producer_script.terminate()
        consumer_gp1_script.terminate()
        consumer_gp2_script.terminate()
        #admin_script.terminate()
        flask_server.terminate()
        websocket_server.terminate()

if __name__ == '__main__':
    main()
