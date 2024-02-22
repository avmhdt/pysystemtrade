from sysdata.csv.csv_futures_contract_prices import ConfigCsvFuturesPrices
import os
from syscore.fileutils import (
    get_resolved_pathname,
    files_with_extension_in_resolved_pathname,
)
from syscore.dateutils import month_from_contract_letter

from sysinit.futures.contract_prices_from_csv_to_arctic import (
    init_arctic_with_csv_futures_contract_prices,
)

from data.bcutils.ze_config import CONTRACT_MAP, INSTRUMENTS_I_TRADE

def strip_file_names(pathname):
    # These won't have .csv attached
    resolved_pathname = get_resolved_pathname(pathname)
    file_names = files_with_extension_in_resolved_pathname(resolved_pathname)
    for filename in file_names:
        identifier = filename.split("_")[0]
        yearcode = int(identifier[len(identifier) - 2 :])
        monthcode = identifier[len(identifier) - 3].upper()
        if yearcode > 50:
            year = 1900 + yearcode
        else:
            year = 2000 + yearcode
        month = month_from_contract_letter(monthcode)
        marketcode = identifier[: len(identifier) - 3].upper()
        instrument = market_map[marketcode]

        datecode = str(year) + "{0:02d}".format(month)

        new_file_name = "%s_%s00.csv" % (instrument, datecode)
        new_full_name = os.path.join(resolved_pathname, new_file_name)
        old_full_name = os.path.join(resolved_pathname, filename + ".csv")
        print("Rename %s to\n %s" % (old_full_name, new_full_name))

        os.rename(old_full_name, new_full_name)

    return None


market_map = {
    k: v['code'] for k, v in CONTRACT_MAP.items() if k in INSTRUMENTS_I_TRADE and v['code'] is not None
}


barchart_csv_config = ConfigCsvFuturesPrices(
    input_date_index_name="Time",
    input_skiprows=0,
    input_skipfooter=1,
    input_date_format="%Y-%m-%dT%H:%M:%S%z",    #"%m/%d/%Y",
    input_column_mapping=dict(
        OPEN="Open", HIGH="High", LOW="Low", FINAL="Close", VOLUME="Volume"
    ),
)


def transfer_barchart_prices_to_arctic(datapath):
    # strip_file_names(datapath)
    init_arctic_with_csv_futures_contract_prices(
        datapath, csv_config=barchart_csv_config
    )


if __name__ == "__main__":
    input("Will overwrite existing prices are you sure?! CTL-C to abort")
    # modify flags as required
    datapath = 'data.bcutils.data'
    transfer_barchart_prices_to_arctic(datapath)
