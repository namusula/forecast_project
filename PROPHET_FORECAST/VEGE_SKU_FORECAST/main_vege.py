from yecai_prophet import Trainer

if __name__=="__main__":
    trainer = Trainer()
    data = trainer.main()
    trainer.write_hive(data)
    print("mission completed")