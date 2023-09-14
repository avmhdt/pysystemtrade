import pandas as pd
from syscore.constants import arg_not_supplied

from sysdata.csv.csv_futures_contract_prices import (
    csvFuturesContractPriceData,
    ConfigCsvFuturesPrices,
)
from sysdata.arctic.arctic_futures_per_contract_prices import (
    arcticFuturesContractPriceData,
)
from sysobjects.contracts import futuresContract
from syscore.fileutils import resolve_path_and_filename_for_package

ESIGNALCSVCOLMAP = {'OPEN': 'Open',
                    'HIGH': 'High',
                    'LOW': 'Low',
                    'FINAL': 'Close',
                    'VOLUME': 'Vol',
                    # 'OI': 'OI'
                    }

CONTRACT_MONTH_CODES = dict(zip(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                                "FGHJKMNQUVXZ"
                                )
                            )


def init_arctic_with_csv_futures_contract_prices(
    datapath: str, configpath: str
):
    instrument_config = pd.read_csv(
        resolve_path_and_filename_for_package(
            configpath + '.instrumentconfig_with_adjustment.csv'
        ),
        index_col=[0]
    )

    input(
        "WARNING THIS WILL ERASE ANY EXISTING ARCTIC PRICES WITH DATA FROM %s ARE YOU SURE?! (CTRL-C TO STOP)"
        % datapath
    )
    for instrument_code, row in instrument_config.iterrows():
        esignal_csv_config = ConfigCsvFuturesPrices(
            input_date_index_name='Date',
            input_date_format='%m/%d/%Y',
            input_column_mapping=ESIGNALCSVCOLMAP,
            input_skiprows=0,
            input_skipfooter=0,
            apply_multiplier=row.Adjustment,
            apply_inverse=False
        )
        init_arctic_with_csv_futures_contract_prices_for_code(
            instrument_code,
            '%s.%s' % (datapath, instrument_code),
            csv_config=esignal_csv_config
        )


def init_arctic_with_csv_futures_contract_prices_for_code(
    instrument_code: str, datapath: str, csv_config=arg_not_supplied
):
    print(instrument_code)
    csv_prices = csvFuturesContractPriceData(datapath, config=csv_config)
    arctic_prices = arcticFuturesContractPriceData()

    print("Getting .csv prices may take some time")
    try:
        csv_price_dict = csv_prices.get_merged_prices_for_instrument(instrument_code)
    except:
        print("******************** {}: ERROR - NO FOLDER FOUND ********************".format(instrument_code))
        return

    print("Have .csv prices for the following contracts:")
    print(str(csv_price_dict.keys()))

    for contract_date_str, prices_for_contract in csv_price_dict.items():
        print("Processing %s" % contract_date_str)
        print(".csv prices are \n %s" % str(prices_for_contract))
        contract = futuresContract(instrument_code, contract_date_str)
        print("Contract object is %s" % str(contract))
        print("Writing to arctic")
        arctic_prices.write_merged_prices_for_contract_object(
            contract, prices_for_contract, ignore_duplication=True
        )
        print("Reading back prices from arctic to check")
        written_prices = arctic_prices.get_merged_prices_for_contract_object(contract)
        print("Read back prices are \n %s" % str(written_prices))


if __name__ == "__main__":
    input("Will overwrite existing prices are you sure?! CTL-C to abort")
    # modify flags as required
    datapath = "data.futures.individual_contracts"
    configpath = "data.futures.csvconfig"
    init_arctic_with_csv_futures_contract_prices(datapath, configpath)
