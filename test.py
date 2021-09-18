import configparser
import logging
import os, sys, time
from datetime import datetime
from threading import Thread
from zipfile import ZipFile
from os import path
import psutil


# Создает архив(filename.zip)
# папки создается автоматиески, есои ее нет.
def zipFile(file, file_path, file_path_zip):
    if file_path_zip == None:
        file_path_zip = file_path
    if path.exists(file_path):
        if not path.exists(file_path_zip):
            os.makedirs(file_path_zip)
        os.chdir(file_path)
        with ZipFile(f"{file_path_zip}/{file}.zip", "w") as newzip:
            newzip.write(file)
            newzip.close()


# Проверка данных, если дата содания файла больше N дней то функция возраает 1
def checkFile(abspath_file, cutoff):
    if os.path.isfile(abspath_file):
        t = os.stat(abspath_file)
        c = t.st_ctime
        if c < cutoff:
            return 1
        return 0


# Архивирут данные, если прошло N дней
def start_zip_file(storage_dir, archive_dir, cutoff, threads):
    for root, dirs, files in os.walk(storage_dir):
        for file in files:
            if file:
                os.chdir(root)
                if checkFile(os.path.abspath(file), cutoff) == 1:
                    log.info(f"Cработало условие!!!\"Создания файла {file} больше 90 дней\"")
                    zip_path = f"{archive_dir}/{os.path.relpath(root, storage_dir)}"
                    log.info(f"create zip {zip_path}/{file}")
                    threads[file] = {"thread": Thread(target=zipFile(file, root, zip_path)), "path_file": root}
                    threads[file]["thread"].start()
                    log.info(f"zip ok  {zip_path}/{file}")
    return threads


# логи
def get_logger(name=__file__, file='log.txt', encoding='utf-8'):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-8s %(message)s')

    fh = logging.FileHandler(file, encoding=encoding)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    log.addHandler(sh)

    return log


if __name__ == '__main__':
    path_conf = f"{os.getcwd()}/conf.ini"
    config = configparser.ConfigParser()
    config.read(path_conf)
    threads = {}
    log = get_logger()
    print = log.info
    storage_dir = f"{str(config.get('Settings','storage_dir'))}"
    archive_dir = f"{str(config.get('Settings', 'archive_dir'))}"
    N = int(config.get('Settings', 'N') ) # дни
    sec = int(config.get('Settings', 'sec') )
    DISK = config.get('Settings', 'DISK')
    test = int(config.get('Settings', 'test'))
    now = time.time()
    cutoff = now - (N * sec) #90 дней
    min = psutil.disk_usage(DISK).total / (1024 * 1024 * 1024) * 10 / 100
    free = psutil.disk_usage(DISK).free / (1024 * 1024 * 1024)

    if free < min or test == 1:
        if test == 1:
            log.info("Тест.")
        log.info("Cработало условие!!!\"Место на диске меньше 10%\"")
        threads = start_zip_file(storage_dir, archive_dir, cutoff, threads)
        s = 0
        time_limit = 1
        if threads:
            while s <= len(threads):
                for file in threads:
                    start = datetime.now()
                    if os.path.exists(f"{threads[file]['path_file']}/{file}") == True:
                        while (datetime.now() - start).seconds <= time_limit:
                            threads[file]["thread"].join()
                            if os.path.exists(f"{threads[file]['path_file']}/{file}") == True:
                                os.remove(f"{threads[file]['path_file']}/{file}")
                                log.info(f"remove {threads[file]['path_file']}/{file}")
                                break
                s = s + 1

