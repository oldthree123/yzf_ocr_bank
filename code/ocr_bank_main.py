import time
import pymysql
import multiprocessing
from ocr_bank_get_task import *
from ocr_bank_handle_task import *


def get_task_while(root_queue):
	"""
	func: 不断获取任务
	params:
		root_queue: 进程间的通信，用于存放任务
	return:
	"""
	config_ocr = load(open(r'./config_ocr.yml', encoding='utf-8'))
	maxsize = config_ocr['task_msg']['maxsize']
	while True:
		get_task_main(root_queue, maxsize)


def handle_task_while(root_queue):
	"""
	fuc: 不断的处理任务进程
	params:
		root_queue: 进程间的通信，用于存放任务
	return:
	"""
	flag = True
	while flag:
		try:
			BankTicketOCR.get_database_table()
			flag = False
		except Exception as err:
			print("连接数据库错误")
			print(err)
			time.sleep(10)

	while True:
		handle_task_main(root_queue)


def main():

	root_queue = multiprocessing.Queue()
	p_get_task = multiprocessing.Process(target=get_task_while, args=(root_queue,))
	p_handle_task = multiprocessing.Process(target=handle_task_while, args=(root_queue,))

	p_get_task.start()
	p_handle_task.start()

	p_get_task.join()
	p_handle_task.join()



if __name__ == '__main__':
	print("*****任务启动*****")
	main()
