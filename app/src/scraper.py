import re
import os
import time
import threading

import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
import openpyxl
import xlsxwriter

from bs4 import BeautifulSoup

from geo_and_xlsx_conversion import process_folder
from constants import DEFAULT_HEADERS, BASE_URL, REPLACE_DICT, INPUT_FOLDER, OUTPUT_FOLDER


class Parameters:

    def __init__(self):
        response = requests.get(BASE_URL, headers=DEFAULT_HEADERS)
        self.html = response.text

        self.soup = BeautifulSoup(response.text, 'lxml')

        self.opstina_list = self.get_opstina_list()
        self.filter_list = self.get_filters()
        self.VIEWSTATE, self.VIEWSTATEGENERATOR, self.EVENTVALIDATION = self.get_hashes(self.html)
        print()

    def get_opstina_list(self):
        # get options from '--- Opstina ---'
        select_list = self.soup.find_all('select')
        opstina_fieldset = select_list[0].find_all('option')
        opstina_fieldset = opstina_fieldset[1:len(opstina_fieldset)]
        opstina_list = [option['value'] for option in opstina_fieldset]
        print(f'found {len(opstina_list)} options in field "Opstina"')
        return opstina_list

    def get_filters(self):
        dd_list = self.soup.find_all('fieldset')[2].find_all('dd')

        # get options from '--- PrometZemljiste  ---'
        zeml_fieldset = dd_list[0].find('span').find_all('input')
        zeml_list = [input['value'] for input in zeml_fieldset]
        print(f'found {len(zeml_list)} options in field "PrometZemljiste"')

        # get options from '--- PrometObjekti  ---'
        object_fieldset = dd_list[1].find('span').find_all('input')
        object_list = [input['value'] for input in object_fieldset]
        print(f'found {len(object_list)} options in field "PrometObjekti"')

        # get options from '--- PrometPosebniDelovi  ---'
        delov_fieldset = dd_list[2].find('span').find_all('input')
        delov_list = [input['value'] for input in delov_fieldset]
        print(f'found {len(delov_list)} options in field "PrometPosebniDelovi"')

        return zeml_list + object_list + delov_list

    def get_hashes(self, html):
        __VIEWSTATE = re.findall(r'id="__VIEWSTATE" value="(.*?)"', self.html)[0]
        __VIEWSTATEGENERATOR = re.findall(r'id="__VIEWSTATEGENERATOR" value="(.*?)"', self.html)[0]
        __EVENTVALIDATION = re.findall(r'id="__EVENTVALIDATION" value="(.*?)"', self.html)[0]

        return __VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION


class Utils:

    @staticmethod
    def to_url_parameter(input_string):
        return urllib.parse.quote(input_string, safe='')

    @staticmethod
    def drop_dupl(df, path):
        df = pd.read_csv(path, delimiter=',')
        df = df.drop_duplicates()
        data_csv = df.to_csv(path, index=False)
        return data_csv

    @staticmethod
    def translate_info(info):
        replace_dict = REPLACE_DICT
        replace_dict.update({k.upper(): v.upper() for k, v in replace_dict.items()})
        info = info.translate({ord(k): v for k, v in replace_dict.items()})
        return info


class Scraper:

    def __init__(self):
        self.parameters = Parameters()

    def get_body_with_hashes(self, start_date, finish_date, opst):
        body_with_hashes = {
            'ctl04': 'ctl17%7COpstina',
            '__EVENTTARGET': 'Opstina',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': self.parameters.VIEWSTATE,
            '__VIEWSTATEGENERATOR': self.parameters.VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': self.parameters.EVENTVALIDATION,
            'DatumPocetak': start_date,
            'DatumZavrsetak': finish_date,
            'Opstina': opst,
            'KatastarskaOpstina': '-1',
            '__ASYNCPOST': 'true'
        }
        return body_with_hashes

    def get_kat_opstina_list(self, start_date, finish_date, opst):
        body = self.get_body_with_hashes(start_date, finish_date, opst)
        body = "&".join([f'{k}={Utils.to_url_parameter(v)}' for k, v in body.items()]) + "&"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Requested-With': 'XMLHttpRequest',
            'X-MicrosoftAjax': 'Delta=true',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8',
            'Origin': 'https://katastar.rgz.gov.rs',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://katastar.rgz.gov.rs/RegistarCenaNepokretnosti/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
        }
        response = requests.post(BASE_URL, headers=headers, data=body)
        soup = BeautifulSoup(response.text, 'lxml')

        kat_opstina_fieldset = soup.select_one('select[name="KatastarskaOpstina"]').select('option')
        kat_opstina_list = [option['value'] for option in kat_opstina_fieldset]
        return kat_opstina_list

    @staticmethod
    def parse_data(raw_data):

        parser_data = []
        for contract_data in raw_data.values():
            for n in range(0, len(contract_data['n'])):
                lat = contract_data['n'][n]['latlon']['Lat']
                lon = contract_data['n'][n]['latlon']['Lon']
                pov = contract_data['n'][n]['pov']
                if not pov:
                    pov = '-'

                price = str(contract_data['cena'])
                currency = str(contract_data['cenaV'])

                row = {
                    'contract ID': str(contract_data['uID']),
                    'date': str(contract_data['datumU']),
                    'contract type': Utils.translate_info(str(contract_data['ppNaziv'])),
                    'contract description': Utils.translate_info(str(contract_data['vPromNaziv'])),
                    'contract price': price,
                    'currency': currency,
                    'object ID': str(contract_data['n'][n]['pID']),
                    'object category': Utils.translate_info(str(contract_data['n'][n]['vNepNaziv'])),
                    'pov': str(pov),
                    'latitude': str(lat),
                    'longitude': str(lon),
                }
                parser_data.append(row)
        return pd.DataFrame(parser_data)

    def collect_year_data(self, year):
        """
        Get data from a year and save it.

        :param year: int
        :return: None
        """
        result_filepath_year_data = Path(f"../data/contracts_{year}.csv")
        print(f'Started collecting data from {year} year')
        start_date, finish_date = f"01.01.{year}", f"31.12.{year}"

        # get data for given year
        # iterate for opstina
        period_df = pd.DataFrame()
        counter = 0
        for opst in self.parameters.opstina_list:
            filepath_year_data = Path(f"../data/contracts_{year}_{opst}.csv")
            if filepath_year_data.exists():
                print(f'Skip collecting data for {year}/{opst}')

                try:
                    period_df = pd.read_csv(filepath_year_data)
                except Exception as e:
                    if "No columns to parse from file" not in str(e):
                        raise e
                counter += 1
                continue

            # iterate for katarska_opstina
            kat_opstina_list = self.get_kat_opstina_list(start_date, finish_date, opst)
            kat_len = len(kat_opstina_list)
            kat_counter = 0
            for kat_opst in kat_opstina_list:
                kat_counter += 1
                url = 'https://katastar.rgz.gov.rs/RegistarCenaNepokretnosti/Default.aspx/Data'
                body = {
                    "DatumPocetak": start_date,
                    "DatumZavrsetak": finish_date,
                    "OpstinaID": opst,
                    "KoID": kat_opst,
                    "VrsteNepokretnosti": ",".join(self.parameters.filter_list)
                }
                response = requests.post(url, headers=DEFAULT_HEADERS, json=body)
                data = response.json()

                data = data["d"]["Ugovori"]
                part_df = self.parse_data(data)

                print(f"Adding items to {start_date} -- {finish_date} period. "
                      f"{opst} :: {kat_counter:03d}/{kat_len:03d} :: "
                      f"{len(part_df):03d}")
                period_df = pd.concat([period_df, part_df], ignore_index=True)

            print(f'\n\n{year}/{opst} has been processed. Collected {len(period_df)} items')
            period_df.to_csv(filepath_year_data, index=False)
            for file in Path("../data").glob(f'opstina_{year}_status_*.txt'):
                file.unlink()
            counter += 1
            Path(f"../data/opstina_{year}_status_{counter}_from_{len(self.parameters.opstina_list)}.txt").touch()

            # Clear past files
            for f_ in Path("../data").glob(f'contracts_{year}*.csv'):
                if f_ != filepath_year_data:
                    f_.unlink()
                    f_.touch()

        # Save Year's data
        period_df = period_df.drop_duplicates()
        period_df.to_csv(result_filepath_year_data, index=False)

        # Remove all the temporary files
        for f_ in Path("../data").glob(f'contracts_{year}*.csv'):
            if f_ != result_filepath_year_data:
                f_.unlink()

    def collect_old_data(self):
        """
        It collects old data for previously dates until now if data haven't been collected early.

        :return: None
        """
        # Create a list to hold the threads
        threads = []

        # Create threads for each task
        updated_mark = False
        for year in range(2012, datetime.now().year+1):
            result_filepath_year_data = Path(f"../data/contracts_{year}.csv")
            if result_filepath_year_data.exists():
                print(f'Skip collecting data for {year}')
                continue

            updated_mark = True
            thread = threading.Thread(target=self.collect_year_data, args=(year,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        print("All years are processed.")

        self.check_files()

        print('started geodata collecting process')
        created_file_mark = process_folder(INPUT_FOLDER, OUTPUT_FOLDER, max_workers=2)
        if created_file_mark != 0:
            self.get_result_file()

        return updated_mark

    @staticmethod
    def check_files():
        filepath_old_result_csv = Path("../data/contracts.csv")
        filepath_old_result_csv.unlink(missing_ok=True)
        print('file "contracts.csv" successfully deleted')
        # for f_ in Path("../data").glob(f'contracts_*.csv'):
        #     df = pd.read_csv(f_)
        #     if 'location' in df.columns:
        #         del df['location']
        #         df.to_csv(f_, index=False)

    @staticmethod
    def get_result_file():
        filepath_data_xlsx = OUTPUT_FOLDER.joinpath("contracts.xlsx")
        if os.path.exists(filepath_data_xlsx):
            filepath_data_xlsx.unlink()
        print('Creating result file')
        with pd.ExcelWriter(filepath_data_xlsx) as writer:
            for f_ in OUTPUT_FOLDER.glob(f'contracts_*.xlsx'):
                print(f"Creating {f_.name} list")
                pd.read_excel(f_).to_excel(writer, sheet_name=f"{f_.name.split('_')[1]}", index=False)

    def update_data(self):
        print('Updating data')
        year = datetime.now().year
        filepath_last_year = Path(f"../data/contracts_{year}.csv")
        last_year_xlsx_name = f"contracts_{year}_with_location.xlsx"
        filepath_last_year_xlsx = OUTPUT_FOLDER.joinpath(last_year_xlsx_name)
        filepath_last_year.unlink(missing_ok=True)
        filepath_last_year_xlsx.unlink(missing_ok=True)
        self.collect_year_data(year)
        print(f'started {year} geodata collecting process')
        process_folder(INPUT_FOLDER, OUTPUT_FOLDER, max_workers=2)
        self.get_result_file()
        print(f'Updated {year} year')

