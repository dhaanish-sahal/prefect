from prefect import flow

@flow(log_prints=True)
def hello():
    print("Hello!")

if __name__ == "__main__":
    hello()  # Call the flow         
