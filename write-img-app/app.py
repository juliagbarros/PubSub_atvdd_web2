from PIL import Image, ImageDraw
from confluent_kafka import Consumer, KafkaError
import json
import os
from time import sleep
import logging
OUT_FOLDER = '/processed/write-img/'
NEW = '_write-img'
IN_FOLDER = "/appdata/static/uploads/"

def  write_filename(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    img = Image.open(path_file)
    draw = ImageDraw.Draw(img)
    draw.text((0, 0), filename, fill='white', font_size=150)

    name, ext = os.path.splitext(filename)
    img.save(output_folder + name + NEW + ext)

#sleep(30)
### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'filename-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])
#{"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg"}

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            logging.warning(f"READING {filename}")
            write_filename(IN_FOLDER + filename)
            logging.warning (f"ENDING {filename}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
