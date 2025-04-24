import datetime
import os
import time
import schedule

from pathlib import Path

from scraper import Scraper


class App:

    def run(self):
        """
        It collects old data for previously dates until now and runs circle for updating data.
        :return: None
        """
        # create directory "data"
        os.makedirs(Path("../data/"), exist_ok=True)

        scraper = Scraper()
        updated_mark = scraper.collect_old_data()
        if not updated_mark:
            scraper.update_data()

        # get new data every week
        schedule.every().monday.at("00:00").do(scraper.update_data)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    try:
        app = App()
        app.run()
    except Exception as _ex:
        # raise _ex
        print(f"Exception: {_ex}")
