import gzip
import struct
import datetime
import pandas as pd
import os
from tqdm import tqdm

import argparse

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create file handlers
if not os.path.exists('logs'):
    os.makedirs('logs')

txt_handler = logging.FileHandler('logs//vwap.log')
txt_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
json_formatter = logging.Formatter('%(message)s')
txt_handler.setFormatter(txt_formatter)
logger.addHandler(txt_handler)

def convert_nanosecs_to_datetime(stamp):
    time = datetime.datetime.fromtimestamp(stamp / 1e9)
    return time.strftime('%H:%M:%S')

def parse_trade_message(bin_data):
    data = {
        'message_type' : b'P',
        'stock_locate' : bin_data.read(2),
        'tracking_number' : bin_data.read(2),
        'timestamp' : bin_data.read(6),
        'order_ref_no': bin_data.read(8),
        'buy_sell_indicator': bin_data.read(1),
        'shares': bin_data.read(4),
        'stock' : bin_data.read(8),
        'price': bin_data.read(4),
        'match_no': bin_data.read(8)
    }

    parsed_data = {}
    parsed_data['message_type'] = str(data['message_type'].decode('ascii'))
    parsed_data['stock_locate'] = str(struct.unpack('>H', data['stock_locate'])[0])  # 2-byte unsigned int
    parsed_data['tracking_number'] = str(struct.unpack('>H', data['tracking_number'])[0])  # 2-byte unsigned int

    stamp = struct.unpack('>Q', b'\x00\x00' + data['timestamp'])[0]  # 6-byte nanoseconds -> 8-byte
    parsed_data['timestamp'] = convert_nanosecs_to_datetime(stamp)

    parsed_data['order_ref_no'] = str(struct.unpack('>Q', data['order_ref_no'])[0])  # 8-byte unsigned int
    parsed_data['buy_sell_indicator'] = str(data['buy_sell_indicator'].decode('ascii'))  # ASCII character
    parsed_data['shares'] = int(struct.unpack('>I', data['shares'])[0])  # 4-byte unsigned int

    temp_stock = struct.unpack('>8c', data['stock'])
    stock_bytes = b''.join(temp_stock)
    try:
        parsed_data['stock'] = str(stock_bytes.decode('ascii').strip())
    except:
        logging.info(f"Unable to parse stockname{stock_bytes}")
        parsed_data['stock'] = None

    parsed_data['price'] = float(struct.unpack('>I', data['price'])[0]) / 10000.0  # 4-byte price with 4 decimal places
    parsed_data['match_no'] = str(struct.unpack('>Q', data['match_no'])[0])  # 8-byte unsigned int

    return parsed_data, parsed_data['timestamp'].split(':')[0]

def calculate_vwap(df: pd.DataFrame):
    df['amount'] = df['price'] * df['volume']
    df['time'] = pd.to_datetime(df['time'])
    grouped = df.groupby([df['time'].dt.hour, df['symbol']]).agg({'amount': 'sum', 'volume': 'sum'})
    grouped['vwap'] = (grouped['amount'] / grouped['volume']).round(2)
    grouped.reset_index(inplace=True)
    grouped['time'] = grouped['time'].apply(lambda x: f'{x}:00:00')
    return grouped[['time', 'symbol', 'vwap']]

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_name', default='01302019.NASDAQ_ITCH50.gz', help='Write filename of compressed file')
    args = parser.parse_args()

    if not os.path.exists('output'):
        os.makedirs('output')

    with gzip.open(args.file_name, 'rb') as bin_data:
        pbar = tqdm()
        temp_data = []
        df_data = []
        flag = None

        msg_header = bin_data.read(1)
        while True:
            # msg_header = bin_data.read(1)
            if not msg_header:
                break

            if msg_header == b"S":
                bin_data.read(11)
            elif msg_header == b"R":
                bin_data.read(38)
            elif msg_header == b"H":
                bin_data.read(24)
            elif msg_header == b"Y":
                bin_data.read(19)
            elif msg_header == b"L":
                bin_data.read(25)
            elif msg_header == b"V":
                bin_data.read(34)
            elif msg_header == b"W":
                bin_data.read(11)
            elif msg_header == b"K":
                bin_data.read(27)
            elif msg_header == b"A":
                bin_data.read(35)
            elif msg_header == b"F":
                bin_data.read(39)
            elif msg_header == b"E":
                bin_data.read(30)
            elif msg_header == b"C":
                bin_data.read(35)
            elif msg_header == b"X":
                bin_data.read(22)
            elif msg_header == b"D":
                bin_data.read(18)
            elif msg_header == b"U":
                bin_data.read(34)
            elif msg_header == b"P":
                try:
                    readable_data, hour = parse_trade_message(bin_data)

                    if flag is None:
                        flag = hour
                    
                    # When hour changes, process VWAP
                    if flag != hour:
                        df_temp = pd.DataFrame(temp_data, columns=['time', 'symbol', 'price', 'volume'])
                        if not df_temp.empty:
                            result = calculate_vwap(df_temp)
                            result.to_csv(os.path.join('output', str(flag) + '.txt'), sep=' ', index=False, mode='a',
                                          header= not os.path.exists(os.path.join('output', str(flag) + '.txt')))
                            df_data.append(result)
                        temp_data = []  # Reset temp_data for the new hour
                        flag = hour

                    # Append the current data
                    temp_data.append([
                        readable_data['timestamp'],   # timestamp   
                        readable_data['stock'],       # stock name   
                        readable_data['price'],       # price   
                        readable_data['shares']       # volume   
                    ])

                except Exception as e:
                    logging.info(f"Error parsing message: {e}")

            elif msg_header == b"Q":
                bin_data.read(39)
            elif msg_header == b"B":
                bin_data.read(18)
            elif msg_header == b"I":
                bin_data.read(49)
            elif msg_header == b"N":
                bin_data.read(19)

            pbar.update(1)
            msg_header = bin_data.read(1)

        # Process the last batch of data after the loop
        if temp_data:
            df_temp = pd.DataFrame(temp_data, columns=['time', 'symbol', 'price', 'volume'])
            if not df_temp.empty:
                result = calculate_vwap(df_temp)
                result.to_csv(os.path.join('output', str(flag) + '.txt'), sep=' ', index=False, mode='a', header= not os.path.exists(os.path.join('output', str(flag) + '.txt')))
                df_data.append(result)
