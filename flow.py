from prefect import flow

#this is new commit
@flow(log_prints=True)
def hello():
  print("Hello!")

if __name__ == "__main__":
    hello.deploy(
        name="my-deployment",
    )
