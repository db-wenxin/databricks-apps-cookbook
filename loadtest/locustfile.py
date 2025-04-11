from locust import HttpUser, task, between, events
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AppUser(HttpUser):
   wait_time = between(1, 3)
   
   @task
   def access_app(self):
       start_time = time.time()
       response = self.client.get("/")  # Replace with your app path
       response_time = (time.time() - start_time) * 1000
       
       # Log response time and status code
       if response.status_code != 200:
           logger.warning(f"Request failed: {response.status_code}, time: {response_time}ms")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
   logger.info("Test started - incrementing load to determine maximum connection capacity")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
   logger.info(f"Test completed - final user count: {environment.runner.user_count}")