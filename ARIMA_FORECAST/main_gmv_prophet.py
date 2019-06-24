from gmv_forecast_prophet import Trainer
import time

if __name__=="__main__":

    trainer = Trainer()

    warehouse_data = trainer.main()
    # print(warehouse_data)

    trainer.write_hive(warehouse_data)

    print("MISSION COMPLETED")