import requests
import multiprocessing
import TencentYoutuyun

import os
import time
from io import open
from yaml import load
from requests import exceptions

from ucloud.ufile import downloadufile
from ucloud.logger import set_log_file


class Get_task(object):
	# 获得配置参数
	__config_ocr = load(open(r'./config_ocr.yml', encoding='utf-8'))

	def get_task_from_java(self):
		"""
		func: 从获取java服务器获取任务
		params:
		return:
			r_task (dict): 从java服务器获得的任务结果
		"""
		config_ocr = Get_task.__config_ocr
		taskSize = config_ocr['task_msg']['taskSize']
		geturl = config_ocr['task_msg']['url']['geturl']
		sbyqbb = config_ocr['version_msg']['sbyqbb']
		params = {'taskSize': taskSize, 'sbyqbb': sbyqbb}
		try:
			r = requests.get(geturl, params=params)
			if r.status_code == 200:
				r_task = r.json()
			else:
				r_task = {"success": False}
		except (exceptions.ConnectionError, TimeoutError, exceptions.Timeout,
                exceptions.ConnectTimeout, exceptions.ReadTimeout):
			print("连接java服务器失败")
			time.sleep(30)
			r_task = {"success": False}
		return r_task

	def get_file_from_ufile(self, r_task):
		"""
		func: 通过r_task参数获得任务在ufile上的url，后期可以提供url给腾讯优图直接上传图片
		params:
			r_task (dict): get_task_from_java函数返回的参数,从java服务端返回的任务结果
		return:
			file_msgs (list) : 得到任务里的所有的文件的信息，包含
									file_tpids :  id
									file_urls :  url
									file_names :  名字加后缀
									file_qymcs : 企业名称
		"""
		file_urls = list()
		file_tpids = list()
		file_names = list()
		file_qymcs = list()
		# 设置完整本地日志文件名
		localloggame = Get_task.__config_ocr['log_msg']['ufile_log']
		set_log_file(localloggame)
		results = r_task['result']
		for result in results:
			public_key = result['UCloudPublicKey']
			private_key = result['UCloudPrivateKey']
			handler = downloadufile.DownloadUFile(public_key, private_key)
			# 目标空间
			bucket = result['bucketName']
			#  目标空间内要下载的文件名（带路径）
			key = result['ucouldtpmc']
			# 文件名字
			file_name = result['ucouldtpmc'].split('/')[-1]
			file_names.append(file_name)
			file_tpids.append(result['tpid'])
			file_qymcs.append(result['qymc'])
			url = handler.private_download_url(bucket, key, expires=300)
			ret = requests.get(url)
			if ret.status_code != 200:
				url = -1
				print("Process: {1}    2. 从Ufile未获得图片url Tpid：{0}".format(result['tpid'], os.getpid()))
			else:
				print("Process: {1}    2. 从Ufile成功获得图片url Tpid：{0}".format(result['tpid'], os.getpid()))
			file_urls.append(url)
		file_msgs = list(zip(file_tpids, file_urls, file_names, file_qymcs))
		return file_msgs

	def get_reponse_from_tencent(self, root_queue, msg, youtu):
		"""
		func: 对file_urls2Tencent函数创建的每一个进程进行腾讯优图识别
		params:
			root_queue (queue): 将腾讯识别结果放入其中
			msg : 图片信息 包含tpid, url, name, qymc
				tpid : 图片id
				url :获取任务之后，从ufile服务器得到可以下载文件的url
				name: 图片名字
				qymc: 企业名称
		return:
		"""
		tpid, url, name, qymc = msg
		if url != -1:
			reponse = youtu.generalocr(url, data_type=1, seq='')
			count = 0
			while (count < 3):
				if reponse['errormsg'] == "OK":
					word_list = list()
					for word_dict in reponse['items']:
						word = word_dict['itemstring'].encode('iso8859-1').decode('utf-8')
						word_list.append(word)
					print("Process: {1}    3. 从TencentYoutuyun成功识别图片 Tpid：{0}".format(tpid, os.getpid()))
					break
				else:
					word_list = -2  # word_list 为-2代表腾讯未正常识别图片
				count += 1
			if word_list == -2:
				print("Process: {1}    3. 从TencentYoutuyun识别图片失败 Tpid：{0}".format(tpid, os.getpid()))
		else:
			word_list = -1  # word_list 为-1代表未获得正确的ufile图片的url
		sb_result = (tpid, qymc, word_list)
		root_queue.put(sb_result)


	def file_urls2Tencent(self, file_msgs, root_queue):
		"""
		func: 根据获得的任务数量，启动n个进程，进行腾讯优图图片识别，识别结果加入root_queue,做任务讲从里面拉取
		params:
			file_msgs: 包含 tpid, url, name, qymc
			tpid : 图片id
			url :获取任务之后，从ufile服务器得到可以下载文件的url
			name: 图片名字
			qymc: 企业名称
		return:
		"""
		# s = requests.session()
		# s.keep_alive = False
		config_ocr = Get_task.__config_ocr
		tencentYoutu_params = config_ocr['tencentYoutu_params']
		appid = tencentYoutu_params['appid']
		secret_id = tencentYoutu_params['secret_id']
		secret_key = tencentYoutu_params['secret_key']
		userid = tencentYoutu_params['userid']
		end_point = TencentYoutuyun.conf.API_YOUTU_END_POINT        # 优图开放平台
		youtu = TencentYoutuyun.YouTu(appid, secret_id, secret_key, userid, end_point)

		names = locals()
		for i, msg in enumerate(file_msgs):
			names['p%s' % i] = multiprocessing.Process(target=self.get_reponse_from_tencent, args=(root_queue, msg, youtu))
			names['p%s' % i] .start()
		for i in range(len(file_msgs)):
			names['p%s' % i].join()


def get_task_main(root_queue, maxsize):
	"""
	func: 取任务的主函数
	params:
		root_queue (queue): 多进程间的通信 （存取任务处理结果的）
	return：
	"""
	if root_queue.qsize() <= maxsize:
		task = Get_task()
		r_task = task.get_task_from_java()
		if r_task['success'] and r_task['result']:
			print("*"*30)
			print("Process: {}   1. 从Java服务器成功获取任务".format(os.getpid()))
			file_msgs = task.get_file_from_ufile(r_task)
			task.file_urls2Tencent(file_msgs, root_queue)
		else:
			time.sleep(5)
