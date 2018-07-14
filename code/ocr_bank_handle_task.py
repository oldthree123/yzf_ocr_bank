# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 17:22:12 2018

@author: dashuai、shangwei
"""
import re
import os
import requests
import numpy as np
import pandas as pd

from io import open
from yaml import load
from pymysql import connect
from xml.dom.minidom import Document


class BankTicketOCR(object):
	# 处于测试时，该值为'Error'，部分错误会返回‘Error’可以区分以进行调试；非测试时改成‘UNK’
	test_or_not = 'UNK'
	# 日期提取时，对于年份的最大限定值，即提取出的年份不能大于这个值
	max_year = '2019'

	@staticmethod
	def get_database_table():
		"""
		fuc: 连接数据库，获得三个数据库表，类型为dataframe,dict,dict;
		params：
		return : 无返回 ，btc,bft_dict,bcaft_dict作为类的属性
		"""
		config_ocr = load(open(r'./config_ocr.yml', encoding='utf-8'))
		host = config_ocr['database']['host']
		db_name = config_ocr['database']['db_name']
		port = config_ocr['database']['port']
		user = config_ocr['database']['user']
		passwd = config_ocr['database']['passwd']
		table_name = config_ocr['database']['table_name']
		conn = connect(host=host, port=port, user=user, passwd=passwd,db=db_name, charset="utf8")
		df_table = dict()
		for key in table_name:
			sql_select = "SELECT * FROM " + table_name[key]
			df_table[key] = (pd.read_sql(sql_select, conn))

		n = len(df_table['bft'])
		m = len(df_table['bcaft'])
		bft_dict = dict()
		bcaft_dict = dict()
		for i in range(n):
			bft_dict[df_table['bft'].iloc[i, 1]] = df_table['bft'].iloc[i, 2]
		for i in range(m):
			bcaft_dict[df_table['bcaft'].iloc[i, 1]] = df_table['bcaft'].iloc[i, 2]
		BankTicketOCR.config_ocr = config_ocr
		BankTicketOCR.btc = df_table['btc']
		BankTicketOCR.bft_dict = bft_dict
		BankTicketOCR.bcaft_dict = bcaft_dict

	def __get_task_from_root_queue(self, root_queue):
		"""
		func : 从root_queue中捞取 取任务进程的数据(root_queue用于两个进程数据通信)
		params :
			root_queue (queue) : 得到的任务经过腾讯优图处理，返回的数据类型为 list
		return : 无 ，添加了实例的属性 包括：
			tpid_total: 图片id
			qymc_total: 企业名称
			word_list_total: 识别结果
		"""
		self.word_list_total = list()
		self.qymc_total = list()
		self.tpid_total = list()
		tasks = list()
		while (not root_queue.empty()):
			tasks.append(root_queue.get())
		for task in tasks:
			tpid, qymc, word_list = task
			self.tpid_total.append(tpid)
			self.qymc_total.append(qymc)
			self.word_list_total.append(word_list)

	def ticket_classification(self, root_queue):
		"""
		fuc: 1.从root_queue中捞取任务；2. 银行票据类别定位
		params:
			root_queue (queue) : 得到的任务经过腾讯优图处理，返回的数据类型为 list
		return:
			bank_ticket_id_list: 银行票据类别id
		"""
		self.__get_task_from_root_queue(root_queue)
		num_picture = len(self.word_list_total)
		bank_ticket_id_list = list()
		for i in range(num_picture):
			word_list = self.word_list_total[i]
			bank_ticket_id = BankTicketOCR.btc.apply(self.__match_bank_ticket_id, axis=1, args=(word_list,))
			bank_ticket_id = bank_ticket_id[bank_ticket_id != 'UNK']
			id_list = bank_ticket_id.unique().tolist()
			if not id_list:
				id_list = 'UNK'
			bank_ticket_id_list.append(id_list)
		return bank_ticket_id_list


	def __match_bank_ticket_id(self, df, word_list):
		"""
		银行票据的类别定位
		:param df: dataframe数据
		:param word_list: 调用腾讯API返回的json文件
		:return: 类别号
		"""
		classification_id = "UNK"
		if word_list != -1 and word_list != -2:
			n = len(word_list)
			chinese_expre = r"[a-zA-Z\u4e00-\u9fa5]+"
			pattern = re.compile(chinese_expre, re.A)
			mark1 = df['ticket_classifi_mark_1']
			mark1_ft = self.__get_fault_tolerant(mark1)
			mark1_pattern = re.compile(mark1_ft, re.A)
			mark_count = df['ticket_classifi_mark_count']
			btc_curr_i = BankTicketOCR.btc[BankTicketOCR.btc['ticket_classifi_mark_1'] == mark1]
			for i in range(n):
				current_word_list = pattern.findall(word_list[i])
				current_word_str = "".join(current_word_list)
				match_mark1 = mark1_pattern.search(current_word_str)
				if match_mark1:
					if mark_count == 1:
						classification_id = df['ID']
						return classification_id
					else:
						mark2_list = btc_curr_i['ticket_classifi_mark_2'].unique().tolist()
						for j in range(i, n):
							current_word_list = pattern.findall(word_list[j])
							current_word_str = "".join(current_word_list)
							for mark2 in mark2_list:
								mark2_ft = self.__get_fault_tolerant(mark2)
								mark2_pattern = re.compile(mark2_ft, re.A)
								match_mark2 = mark2_pattern.search(current_word_str)
								if match_mark2:
									btc_curr_j = btc_curr_i[btc_curr_i['ticket_classifi_mark_2'] == mark2]
									mark_count = btc_curr_j['ticket_classifi_mark_count'].unique().tolist()[0]
									if mark_count == 2:
										classification_id = btc_curr_j['ID'].tolist()[0]
										return classification_id
									elif mark_count == 3:
										mark3_list = btc_curr_j['ticket_classifi_mark_3'].unique().tolist()
										for k in range(j, n):
											current_word_list = pattern.findall(word_list[k])
											current_word_str = "".join(current_word_list)
											for mark3 in mark3_list:
												mark3_ft = self.__get_fault_tolerant(mark3)
												mark3_pattern = re.compile(mark3_ft, re.A)
												match_mark3 = mark3_pattern.search(current_word_str)
												if match_mark3:
													btc_curr_k = btc_curr_j[btc_curr_j['ticket_classifi_mark_3'] == mark3]
													classification_id = btc_curr_k['ID'].tolist()[0]
													return classification_id
		return classification_id

	def __get_fault_tolerant(self, word):
		"""
		func: 对锚点字段进行正则纠错
		params:
			word: 锚点字段
		return: 若锚点字段是‘人民币’,则生成类似'[人大太][民戈][币市]'这样的正则表达式
		"""
		if word != '':
			expre = ""
			for char in word:
				if char in BankTicketOCR.bft_dict.keys():
					expre += "[" + BankTicketOCR.bft_dict[char] + ']'
				else:
					expre += '[' + char + ']'
		else:
			return ''
		return expre

	def __get_fault_tolerant_pay2(self, payer_name='', payer_next='', payee_next=''):
		"""
		func: 生成pay_type=2的纠错字段表达式
		params:
			payer_name: 付款人名称字段
			payer_next: 付款人名称下一个字段
			payee_next: 收款人名称字段
		return: 纠错字段的正则表达式
		"""
		pay_str = payer_name + payer_next + payee_next
		pay_set = set(pay_str)
		word_ft_expre = "("
		for word in pay_set:
			if word in BankTicketOCR.bft_dict.keys():
				word_ft_expre += "[" + BankTicketOCR.bft_dict[word] + ']|'
			else:
				word_ft_expre += "[" + word + ']|'
		word_ft_expre = word_ft_expre[:-1] + ")"
		return word_ft_expre

	def extract_key_word(self, bt_id):
		"""
		除了main之外的程序第二个主体函数，所有银行票据的字段提取都集中在这里
		:return: 所有传进来处理的图片的结果，以dataframe形式展现
		"""
		# 生成空的DataFrame
		key_word_df = pd.DataFrame(columns=('tpid', 'bt_id', 'word_list_total', 'qymc'))
		# DataFrame赋值
		key_word_df['tpid'] = self.tpid_total
		key_word_df['bt_id'] = bt_id
		key_word_df['word_list_total'] = self.word_list_total
		key_word_df['qymc'] = self.qymc_total

		# 字段提取
		key_word_df['pay_payee'] = key_word_df.apply(self.__extract_pay_str, axis=1)
		key_word_df['payer_name'] = key_word_df.apply(lambda x: x['pay_payee'][0] if x['pay_payee'] else np.nan, axis=1)
		key_word_df['payee_name'] = key_word_df.apply(lambda x: x['pay_payee'][1] if x['pay_payee'] else np.nan, axis=1)
		key_word_df['date'] = key_word_df.apply(self.__extract_date, axis=1)
		key_word_df['date'] = key_word_df.apply(self.__handle_date, axis=1)
		key_word_df['capital_amount'] = key_word_df.apply(self.__extract_capital_amount, axis=1)
		key_word_df['capital_amount'] = key_word_df.apply(self.__handle_capital_amount, axis=1)
		key_word_df['lowercase_amount'] = key_word_df.apply(self.__extract_lowercase_amount, axis=1)
		key_word_df['lowercase_amount'] = key_word_df.apply(self.__handle_lowercase_amount, axis=1)
		key_word_df['amount'] = key_word_df.apply(self.__compare_both_amount, axis=1)
		key_word_df['abstract'] = key_word_df.apply(self.__extract_abstract, axis=1)
		key_word_df['money_type'] = "UNK"
		key_word_df['business_type'] = "UNK"
		key_word_df['income_and_expenditure_signs'] = "UNK"
		key_word_df['对方账户名称'] = "UNK"

		# 删除无用列
		del key_word_df['word_list_total']
		del key_word_df['pay_payee']
		del key_word_df['capital_amount']
		del key_word_df['lowercase_amount']
		del key_word_df['qymc']
		# del key_word_df['bt_id']
		results = list()
		n = len(key_word_df)
		key_word_df.rename(columns={'payer_name': '付款人户名', 'payee_name': '收款人户名', 'date': '日期',
										'amount': '金额', 'abstract': '摘要', 'money_type': '币种', 'business_type': '业务类型',
										'income_and_expenditure_signs': '收支标志'}, inplace=True)
		key_word_df.replace('UNK', '', inplace=True)
		for i in range(n):
			results.append(key_word_df.loc[i, :].to_dict())
		return results

	def results_to_xml(self, results):
		xml_str = list()
		config_ocr = BankTicketOCR.config_ocr
		for i, result in enumerate(results):
			tpid = result.pop('tpid')
			bt_id = result.pop('bt_id')
			if bt_id != '':
				print("Process: {1}    4. 正确匹配到银行票据模板 tpid: {0}".format(tpid, os.getpid()))
				m = len(result)
				doc = Document()
				root = doc.createElement('RecognizeResult')  # 创建根元素
				doc.appendChild(root)

				TemplateName = doc.createElement('TemplateName')
				root.appendChild(TemplateName)
				TemplateName.appendChild(doc.createTextNode(config_ocr['version_msg']['mbbs']))

				Field = doc.createElement('Field')
				root.appendChild(Field)
				Field.setAttribute('Size', str(m))

				for j in result:
					FieldData = doc.createElement('FieldData')
					Field.appendChild(FieldData)
					FieldData.setAttribute('ID', j)

					RowData = doc.createElement('RowData')
					FieldData.appendChild(RowData)

					Data = doc.createElement('Data')
					RowData.appendChild(Data)
					Data.setAttribute('Key', j)
					Data.setAttribute('Value', result[j])
				xml_str.append((tpid, root.toxml()))
			else:
				print('Process: {}    4. 未正确匹配银行票据模板(可能不是银行票据或者不支持该类型的票据)'.format(os.getpid()))
				xml_str.append((tpid, ''))
		return xml_str

	def __extract_pay_str(self, df):
		"""
		提取收款人和付款人名称
		:param df: dataframe格式数据
		:return: 收付款人组成的元组
		"""
		# 只有类型已知，且类型唯一确定时，才进行提取操作
		if df['bt_id'] != 'UNK' and len(df['bt_id']) == 1:
			# 取到一些必要的值
			pay_type = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['pay_type'].item()
			payer_name = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['payer_name'].item()
			payer_next = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['payer_next'].item()
			payee_name = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['payee_name'].item()
			payee_next = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['payee_next'].item()
			word_list = df['word_list_total']

			# 一些必要的正则表达式生成
			chinese_expre = r"[\u4e00-\u9fa5]+"
			payer_expre = self.__get_fault_tolerant(payer_name) + "([\u4e00-\u9fa5]*)"
			payee_expre = self.__get_fault_tolerant(payee_name) + "([\u4e00-\u9fa5]*)"
			pattern = re.compile(chinese_expre)
			pattern_payer = re.compile(payer_expre)
			pattern_payee = re.compile(payee_expre)

			# 一些必要的正则表达式生成
			payer_next_expre = "(" + self.__get_fault_tolerant(payer_next) + ")"
			payee_next_expre = "(" + self.__get_fault_tolerant(payee_next) + ")"
			pattern_payer_next = re.compile(payer_next_expre)
			pattern_payee_next = re.compile(payee_next_expre)

			# pay_type=1的按如下逻辑进行
			if pay_type == 1:
				payer = self.__extract_payer_str(word_list, pattern, pattern_payer, pattern_payer_next)
				payee = self.__extract_payee_str(word_list, pattern, pattern_payee, pattern_payee_next)
				return payer, payee

			# pay_type=2的按如下逻辑进行
			elif pay_type == 2:
				n = len(df['word_list_total'])
				pay_next = [payer_next, payee_next]
				flag = 0
				pay = ['UNK', 'UNK']
				word_list2 = list()
				pay2_expre = self.__get_fault_tolerant_pay2(payer_name, payer_next, payee_next)
				pay2_pattern = re.compile(pay2_expre)
				for i in range(n):
					current_word_list = pattern.findall(word_list[i])  # 每一句word只取中文
					current_word_str = "".join(current_word_list)
					con1 = len(current_word_str) < 2
					con2 = pay2_pattern.search(current_word_str)
					if con1 and (not con2):
						pass
					else:
						word_list2.append(current_word_str)

				for i in range(len(word_list2)):
					pay2_match = pattern_payer.search(word_list2[i])
					if pay2_match and flag < 2:
						if len(word_list2[i]) > 2:
							m = word_list2[i].find(payer_name)
							pay[flag] = word_list2[i][m + len(payer_name):]
							flag += 1
						else:
							pay_next_expre = self.__get_fault_tolerant_pay2(payer_next=pay_next[flag])
							pay_next_pattern = re.compile(pay_next_expre)
							if i != len(word_list2) - 1:
								pay_next_match = pay_next_pattern.search(word_list2[i + 1])
								if pay_next_match and pay_next_match.end() < 5:
									pay[flag] = "Empty"
								else:
									pay[flag] = word_list2[i + 1]
								flag += 1
					elif flag >= 2:
						break
				return pay[0], pay[1]

			# pay_type=3的按如下逻辑进行
			# 这种类型的，付款人是银行
			elif pay_type == 3:
				payer = "银行"
				payee = self.__extract_payee_str(word_list, pattern, pattern_payee, pattern_payee_next)
				return payer, payee

			# pay_type=4的按如下逻辑进行
			# 这种类型的，收款人是银行
			elif pay_type == 4:
				payee = "银行"
				payer = self.__extract_payer_str(word_list, pattern, pattern_payer, pattern_payer_next)
				return payer, payee
		# 类型未知或者类型不唯一确定时
		else:
			return df['bt_id'], df['bt_id']

	def __extract_payer_str(self, word_list, pattern, pattern_payer, pattern_payer_next):
		"""
		根据付款人的锚点字段找付款人
		:param word_list: 腾讯返回的json文件提炼而成的word列表
		:param pattern: 对每个word进行提取的正则表达式pattern
		:param pattern_payer: 付款人的锚点字段pattern
		:param pattern_payer_next: 为了判断付款人是否为空的字段pattern
		:return: 付款人字符串
		"""
		payer = 'UNK'
		n = len(word_list)
		for i in range(n):
			# 每一句word只取中文
			current_word_list = pattern.findall(word_list[i])
			current_word_str = "".join(current_word_list)
			# 正则匹配 付款人锚点字段
			match_payer = pattern_payer.search(current_word_str)
			if match_payer:
				# 如果匹配到，则取锚点字段所在word的剩余字符串
				rest_word = match_payer.group(1)
				# 如果剩余字符串不为空
				if len(rest_word) >= 1:
					# 情况2,3,6
					# 如果剩余字符串不为空，则判断剩余字符串中是否有next_word
					match_payer_next = pattern_payer_next.search(rest_word)
					# 匹配到rest_word里有next_word
					if match_payer_next:
						# 情况2,6
						index = rest_word.find(match_payer_next.group())
						# 取到rest_word左边的字符串
						rest_word = rest_word[0:index]
						if len(rest_word) < 1:
							# 情况6
							# 若rest_word左边的字符串为空，则付款人为空
							payer = 'Empty'
						else:
							# 情况2
							# rest_word左边的字符串不为空，则为付款人
							payer = rest_word
					# 匹配到rest_word里没有next_word
					else:
						# 情况3
						# 则剩余字符串即为付款人名称
						payer = rest_word
				# 情况1,4,5
				# 如果剩余字符串为空
				else:
					# 依次找后续1,2,3个word，取第一个不为空的那个word
					next_word_payer = pattern.findall(word_list[i + 1])
					next_word_payer = "".join(next_word_payer)
					if next_word_payer == '':
						for j in range(2, 4):
							next_word_payer = pattern.findall(word_list[i + j])
							next_word_payer = "".join(next_word_payer)
							if next_word_payer != '':
								break

					# 在第一个不为空的word里搜索是否有next_word
					match_payer_next = pattern_payer_next.search(next_word_payer)
					# 如果找到next_word
					if match_payer_next:
						# 情况4,5
						# 如果找到next_word，则取next_word左边的字符串
						index = next_word_payer.find(match_payer_next.group())
						next_word_payer = next_word_payer[0:index]
						# 若左边的字符串为空
						if len(next_word_payer) < 1:
							# 情况5
							payer = 'Empty'
						# 若左边的字符串不为空，则左边的字符串即为付款人名称
						else:
							# 情况4
							payer = next_word_payer
					# 如果没找到next_word
					else:
						# 情况1
						payer = next_word_payer
		return payer

	def __extract_payee_str(self, word_list, pattern, pattern_payee, pattern_payee_next):
		"""
		根据收款人的锚点字段找付收款人，代码及注释完全同extract_payer_str
		:param word_list: 同上
		:param pattern: 同上
		:param pattern_payee: 同上
		:param pattern_payee_next: 同上
		:return: 收款人字符串
		"""
		payee = 'UNK'
		n = len(word_list)
		for i in range(n):
			current_word_list = pattern.findall(word_list[i])
			current_word_str = "".join(current_word_list)
			match_payee = pattern_payee.search(current_word_str)
			if match_payee:
				rest_word = match_payee.group(1)
				if len(rest_word) >= 1:
					match_payee_next = pattern_payee_next.search(rest_word)
					if match_payee_next:
						index = rest_word.find(match_payee_next.group())
						rest_word = rest_word[0:index]
						if len(rest_word) < 1:
							payee = 'Empty'
						else:
							payee = rest_word
					else:
						payee = rest_word
				else:
					next_word_payee = pattern.findall(word_list[i + 1])
					next_word_payee = "".join(next_word_payee)
					if next_word_payee == '':
						for j in range(2, 4):
							next_word_payee = pattern.findall(word_list[i + j])
							next_word_payee = "".join(next_word_payee)
							if next_word_payee != '':
								break
					match_payee_next = pattern_payee_next.search(next_word_payee)
					if match_payee_next:
						index = next_word_payee.find(match_payee_next.group())
						next_word_payee = next_word_payee[0:index]
						if len(next_word_payee) < 1:
							payee = 'Empty'
						else:
							payee = next_word_payee
					else:
						payee = next_word_payee
		return payee

	def __extract_date(self, df):
		"""
		依据配置表里的不同date类型进行日期字段的提取
		:param df: dataframe数据
		:return: 日期字段
		"""
		# 只有类型已知，且类型唯一确定时，才进行提取日期操作
		if df['bt_id'] != 'UNK' and len(df['bt_id']) == 1:
			date_name = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['date_name'].item()
			date_type = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['date_type'].item()
			word_list = df['word_list_total']
			# 有锚点字段的进行如下处理
			if date_type == 1:
				# 为锚点字段生成正则表达式
				compres1 = self.__get_fault_tolerant(date_name)
				# 若锚点字段为‘日期’，则为了避免匹配到‘交易日期’等，必须规定日期打头
				if date_name == '日期':
					compres1 = '^' + compres1
				pattern1 = re.compile(compres1)
				pattern = re.compile(r'[\u4e00-\u9fa5\da-zA-Z]+')
				# 遍历所有word
				for index, item in enumerate(word_list):
					# 对每个word进行处理，只保留中文、数字和英文字母
					curr_list = pattern.findall(item)
					curr_string = ''.join(curr_list)
					# 试图匹配锚点字段
					match1 = pattern1.search(curr_string)
					# 若匹配到锚点字段
					if match1:
						# 则尝试去锚点字段所在word匹配日期
						pattern2 = re.compile(
							r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*('
							r'[01iC][SiCO\d])[\u4e00-\u9fa5Jj]*([0123iCO][SiCO\d])[\u4e00-\u9fa5HF]*')
						match2 = pattern2.search(curr_string)
						# 在锚点字段所在word匹配到日期
						if match2:
							return match2.groups()
						# 没在锚点字段所在word匹配到日期，则去下个word找
						else:
							# 从之后的3个word里找到保留中文、字符和数字之后不为空的那个word
							next_word = word_list[index + 1]
							next_list = pattern.findall(next_word)
							next_string = ''.join(next_list)
							if next_string == '':
								for j in range(2, 4):
									next_word = word_list[index + j]
									next_list = pattern.findall(next_word)
									next_string = ''.join(next_list)
									if next_string != '':
										break
							pattern3 = re.compile(
								r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*('
								r'[01iC][SiCO\d])[\u4e00-\u9fa5Jj]*([0123iCO][SiCO\d])[\u4e00-\u9fa5HF]*')
							match3 = pattern3.search(next_string)
							if match3:
								# 这个word若没匹配到则会再次进入for循环接着下面的查找
								return match3.groups()
						# 只要匹配到锚点字段而又没找到日期，则直接跳出循环
				else:
					# 遍历完所有word没匹配到锚点字段，说明套打打偏了，直接按date_type == 2的方式匹配
					# 不管锚点字段，直接匹配正则表达式
					return self.__extract_date_from_all_words(word_list)
			# 没锚点字段的进行如下处理
			elif date_type == 2:
				return self.__extract_date_from_all_words(word_list)
			# 未知类按以下处理
			else:
				return 'UNK'
		# 类型未知或者类型不唯一确定则返回'UNK'
		return "UNK"

	def __extract_date_from_all_words(self, word_list):
		"""
		从所有word里提取日期
		:param word_list: word组成的列表
		:return: 日期
		"""
		pattern1 = re.compile(
			r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*([01iC][SiCO\d])['
			r'\u4e00-\u9fa5Jj]*([0123iCO][SiCO\d])[\u4e00-\u9fa5HF]*')
		pattern2 = re.compile(r'[\u4e00-\u9fa5\da-zA-Z]+')
		# 遍历所有word，用正则表达式匹配日期
		for item in word_list:
			curr_list = pattern2.findall(item)
			curr_string = ''.join(curr_list)
			# 2000张只有几张张日期的len>12的
			# 若取消这个限制，则会有大量错误，错提取到编码之类的
			if len(curr_string) <= 12 and curr_string != '':
				match = pattern1.search(curr_string)
				if match:
					return match.groups()
		# 若遍历完都没有匹配到，则返回'UNK'
		else:
			return 'UNK'

	def __handle_date(self, df):
		"""
		对extract_date函数处理来的日期进行后处理
		:param df: dataframe数据
		:return: 修改后的日期字符串
		"""
		date = df['date']
		# 若存在日期，则进行以下处理
		if date != 'UNK':
			date_list = list(date)
			# 将匹配到的日期中一些识别错的字符替换成正确的，如201S替换成2015
			for i in range(3):
				date_list[i] = date_list[i].replace("S", "5")
				date_list[i] = date_list[i].replace("i", "1")
				date_list[i] = date_list[i].replace("C", "0")
				date_list[i] = date_list[i].replace("O", "0")
			# 截止目前(2018)，提取到的日期不应该超过以下日期，略有放宽
			if date_list[0] > BankTicketOCR.max_year:
				return 'UNK'
			if date_list[1] > '12':
				return 'UNK'
			if date_list[2] > '31':
				return 'UNK'
			# 将日期组成的列表转成字符串
			date = ''.join(date_list)
		return date

	def __extract_capital_amount(self, df):
		"""
		提取大写金额的中文字符串
		:param df: dataframe数据
		:return: 大写金额的中文字符串
		"""
		# 当类别已知，且类别唯一确定时才进行以下操作
		if df['bt_id'] != 'UNK' and len(df['bt_id']) == 1:
			capital_amount_anchor = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['capital_amount'].item()
			word_list = df['word_list_total']
			n = len(word_list)

			# 当配置表里大写金额为空，即该票不存在大写金额
			if capital_amount_anchor == '':
				return 'Empty'
			# 该票存在大写金额
			else:
				chinese_expre = r"[\u4e00-\u9fa5]+"
				chinese_pattern = re.compile(chinese_expre)
				# 大写金额锚点字段纠错后的正则表达式
				capital_amount_anchor_expre = self.__get_fault_tolerant(capital_amount_anchor)
				capital_amount_anchor_pattern = re.compile(capital_amount_anchor_expre)
				# 生成匹配大写金额的正则表达式，考虑纠错
				capital_amount_expre = "(["
				for value in BankTicketOCR.bcaft_dict.values():
					capital_amount_expre += value
				capital_amount_expre += "]+)"
				capital_amount_pattern = re.compile(capital_amount_expre)

				# 对每句word处理后(只取中文)匹配大写金额的锚点，找到即处理
				for index in range(n):
					current_sentence1 = chinese_pattern.findall(word_list[index])
					current_sentence1 = ''.join(current_sentence1)
					match1 = capital_amount_anchor_pattern.search(current_sentence1)
					if match1:
						# 到该word及其之后的words里去匹配大写中文金额
						for i in range(n - index):
							current_sentence2 = chinese_pattern.findall(word_list[index + i])
							current_sentence2 = ''.join(current_sentence2)
							match2 = capital_amount_pattern.search(current_sentence2)
							if match2:
								return match2.group()
						# 若匹配到锚点字段，但在之后所有word里没匹配到大写金额，则按照没锚点直接正则匹配搜索大写金额
						return self.__second_plan_for_capital_amount(word_list, capital_amount_expre)
				# 遍历所有都没有找到则按照没锚点直接正则匹配搜索大写金额
				else:
					return self.__second_plan_for_capital_amount(word_list, capital_amount_expre)
		# 当类别未知 或者 类别不唯一确定时不进行大写金额字段提取的操作
		else:
			return 'UNK'

	def __second_plan_for_capital_amount(self, word_list, capital_amount_expre):
		"""
		大写金额锚点字段没找到时调用此，不用锚点字段，直接对全部word正则匹配大写金额
		:return: 大写中文金额组成的字符串
		"""
		chinese_expre = r"[\u4e00-\u9fa5]+"
		chinese_pattern = re.compile(chinese_expre)
		capital_amount_pattern = re.compile(capital_amount_expre)

		# 在所有word里正则匹配大写中文金额字段
		for index, item in enumerate(word_list):
			current_sentence = chinese_pattern.findall(word_list[index])
			current_sentence = ''.join(current_sentence)
			match = capital_amount_pattern.search(current_sentence)
			if match:
				return match.group()
		# 遍历完都没有正则匹配到则返回'UNK'
		return 'UNK'

	def __handle_capital_amount(self, df):
		"""
		对上面提取到的大写中文金额字符串转小写
		:param df: dataframe数据
		:return: 小写金额
		"""
		# 大写数字
		number_dict = dict([('壹', 1), ('贰', 2), ('叁', 3), ('肆', 4), ('伍', 5),
							('陆', 6), ('柒', 7), ('捌', 8), ('玖', 9), ('零', 0)])
		# 量词
		quantifier_dict = dict([('圆', 1), ('元', 1), ('拾', 10), ('佰', 100), ('仟', 1000),
								('万', 10000), ('亿', 100000000), ('角', 0.1), ('分', 0.01)])
		string = df['capital_amount']

		if string == 'UNK':
			return 'UNK'
		elif string == 'Empty':
			return 'Empty'
		# 提取到所需文字，进行处理
		else:
			# 首先对返回来的string预处理
			for key, value in BankTicketOCR.bcaft_dict.items():
				for v in value:
					string = string.replace(v, key)

			# 有如下几个规律，只有全部成立，该逻辑才无需更改：
			# 1、大写金额字符串里不存在连续两个大写数字，除了零
			# 2、每个大写数字后都会紧跟一个或多个量词，除了零
			# 3、首个字符一定是大写数字，除了十几元(拾叁元)这种类型
			total_list = list()

			# 若首个字符是'拾'，在之前加个'壹'，这样就避开了'拾'既是量词又是大写数字的矛盾
			if string[0] == '拾':
				string = '壹' + string
			# 若首个字符是除了‘拾’以外的量词，则说明上面的提取大写金额提取错误，有错字
			if string[0] != '拾' and quantifier_dict.get(string[0]) is not None:
				return BankTicketOCR.test_or_not

			n = len(string)
			for index in range(n):
				# 是个大写数字
				if number_dict.get(string[index]) is not None:
					temp_number = number_dict.get(string[index])
					# 存在下个字符(没越界)且下个字符是量词
					if ((index + 2) <= n) and (quantifier_dict.get(string[index + 1]) is not None):
						current_quantifier = quantifier_dict.get(string[index + 1])
						all_quantifier = quantifier_dict.get(string[index + 1])
						# 遍历之后所有，同时避免越界，找出该数字对应的真正量词
						for i in range(2, n - index):
							# 找出index+1之后所有比current_quantifier大的量词，并累积乘数all_quantifier
							if (quantifier_dict.get(string[index + i]) is not None) and \
									(quantifier_dict.get(string[index + i]) > current_quantifier):
								all_quantifier *= quantifier_dict.get(string[index + i])
								current_quantifier = quantifier_dict.get(string[index + i])
						total_list.append(temp_number * all_quantifier)
					# 下个字符越界，则说明识别出了错字
					elif index + 2 > n:
						return BankTicketOCR.test_or_not
					# 数字之后没找到量词，只有如柒万零一佰里零这种可能
					else:
						continue
				# 是个量词就跳过
				elif quantifier_dict.get(string[index]) is not None:
					continue
				# 啥也不是就说明有错字
				else:
					return BankTicketOCR.test_or_not

			# 将列表里每项的值全部加起来
			total_value = 0
			for j, item in enumerate(total_list):
				total_value += item
			# 保留数值的小数点后两位，保证和小写金额匹配时不至于一样的数不相等
			# 如9.350000001 != 9.35
			total_value = format(total_value, '.2f')
			return total_value

	def __extract_lowercase_amount(self, df):
		"""
		提取小写金额
		:param df: dataframe数据
		:return: 小写金额
		"""
		# 当类别已知，且类别唯一确定时才进行以下操作
		if df['bt_id'] != 'UNK' and len(df['bt_id']) == 1:
			lowercase_amount_anchor = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['lowercase_amount'].item()
			word_list = df['word_list_total']
			n = len(word_list)

			# extract_pattern用来保留word中的中文、数字和逗号句号
			# amount_pattern用来匹配金额的数值
			# lowercase_amount_anchor_expre是小写金额锚点字段的纠错正则表达式
			extract_expre = '[\u4e00-\u9fa5,.\d]+'
			extract_pattern = re.compile(extract_expre)
			amount_expre = '([\d,;]+\.\d{2})'
			amount_pattern = re.compile(amount_expre)
			lowercase_amount_anchor_expre = self.__get_fault_tolerant(lowercase_amount_anchor)
			lowercase_amount_anchor_pattern = re.compile(lowercase_amount_anchor_expre)

			for i in range(n):
				# 进行每个word的处理
				curr_string = extract_pattern.findall(word_list[i])
				curr_string = ''.join(curr_string)
				match1 = lowercase_amount_anchor_pattern.search(curr_string)
				if match1:
					# 匹配到小写金额的锚点字段，则从这之后开始进行小写金额的正则匹配
					for j in range(n - i):
						curr_string = extract_pattern.findall(word_list[i + j])
						curr_string = ''.join(curr_string)
						match = amount_pattern.search(curr_string)
						if match:
							return match.group()
					# 遍历完都没有找到小写金额，则直接终止
					return BankTicketOCR.test_or_not
			# 遍历完都没有匹配到锚点字段
			else:
				return BankTicketOCR.test_or_not
		# 当类别未知或者类别不唯一确定时，返回'UNK'
		else:
			return 'UNK'

	def __handle_lowercase_amount(self, df):
		"""
		对上面提取到的小写金额字符串进行处理
		:param df: dataframe数据
		:return: 小写金额
		"""
		lowercase_amount = df['lowercase_amount']
		if lowercase_amount != 'UNK' and lowercase_amount != BankTicketOCR.test_or_not:
			lowercase_amount = lowercase_amount.replace(',', '')
			lowercase_amount = lowercase_amount.replace(';', '')
			lowercase_amount = format(float(lowercase_amount), '.2f')
		return lowercase_amount

	def __compare_both_amount(self, df):
		"""
		综合大写金额和小写金额的情况给出最终的金额
		:param df: dataframe数据
		:return:
		"""
		# 大写金额不为数值
		if df['capital_amount'] == 'Empty' or df['capital_amount'] == BankTicketOCR.test_or_not or df['capital_amount'] == 'UNK':
			if df['lowercase_amount'] != BankTicketOCR.test_or_not and df['lowercase_amount'] != 'UNK':
				return df['lowercase_amount']
			if df['lowercase_amount'] == BankTicketOCR.test_or_not:
				return BankTicketOCR.test_or_not
			if df['lowercase_amount'] == 'UNK':
				return 'UNK'
		# 大写金额为数值
		else:
			return df['capital_amount']

	def __extract_abstract(self, df):
		"""
		提取摘要
		:param df: dataframe数据
		:return: 摘要
		"""
		# 当类别已知，且类别唯一确定时才进行以下操作
		if df['bt_id'] != 'UNK' and len(df['bt_id']) == 1:
			abstract_anchor_str = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['note'].item()
			ticket_types = BankTicketOCR.btc[BankTicketOCR.btc['ID'] == df['bt_id']]['ticket_types'].item()
			# 若从配置表里读出来的abstract为空
			if abstract_anchor_str == '':
				return ticket_types
			# 从配置表里读出来的abstract有值，则对所有word正则处理后找到锚点
			# 再取锚点所在word及其下个word 作为abstract我们所要的值
			else:
				# 将配置表里读出来的abstract值变成列表
				abstract_anchor_list = abstract_anchor_str.split(',')
				word_list = df['word_list_total']
				n = len(word_list)
				total_abstract = list()
				total_word = list()

				# 对每个word进行处理的正则表达式，只保留中文
				chinese_expre = r"[\u4e00-\u9fa5]+"
				chinese_pattern = re.compile(chinese_expre)

				# 遍历所有word并对其进行正则处理,放入total_word中
				for i in range(n):
					curr_list = chinese_pattern.findall(word_list[i])
					curr_str = ''.join(curr_list)
					if curr_str != '':
						total_word.append(curr_str)
				n = len(total_word)

				# 选择一个备注的锚点字段
				for abstract_anchor in abstract_anchor_list:
					# 为列表里所有锚点字段都生成正则表达式
					abstract_anchor_expre = '(' + self.__get_fault_tolerant(abstract_anchor) + ')'
					abstract_anchor_pattern = re.compile(abstract_anchor_expre)
					# 遍历所有word
					for i in range(n):
						match = abstract_anchor_pattern.search(total_word[i])
						if match:
							# 若匹配到锚点字段，则将锚点字段所在word的剩余字段及其下个word都放入total_abstract中
							index = total_word[i].find(match.group())
							# 取到rest_word左边
							rest_word = total_word[i][(index + len(match.group())):]
							if rest_word != '':
								total_abstract.append(rest_word)
							if i <= (n - 2):
								total_abstract.append(total_word[i + 1])
							break
				total_abstract.append(ticket_types)
				# 将列表里的元素连接成字符串
				total_abstract = ''.join(total_abstract)
				return total_abstract
		# 类别未知或者类别不唯一确定
		else:
			return 'UNK'

	def post_result_to_java(self, results):
		config_ocr = BankTicketOCR.config_ocr
		url = config_ocr['task_msg']['url']['posturl']
		sbyqbb = config_ocr['version_msg']['sbyqbb']
		mbbs = config_ocr['version_msg']['mbbs']
		for result in results:
			tpid, sbnr = result
			header = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
			params = {'tpid': tpid, 'sbnr': sbnr, 'sbyqbb': sbyqbb,
						'mbbs': mbbs, 'sfgwjytg': '0', 'gwjyy': '1', 'errorMsg': ''}
			r = requests.post(url, data=params, headers=header)
			if r.status_code == 200:
				print("Process: {}    5. 识别结果成功入库".format(os.getpid()))
			else:
				print("Process: {1}   5. 识别结果未成功入库 状态码：{0}".format(r.status_code,os.getpid()))

def handle_task_main(root_queue):
	ht = BankTicketOCR()
	if not root_queue.empty():
		bt_id = ht.ticket_classification(root_queue)
		results = ht.extract_key_word(bt_id)
		results_xml = ht.results_to_xml(results)
		ht.post_result_to_java(results_xml)


