# -*- coding: utf-8 -*-
"""
Created on Mon May 21 17:06:41 2018

@author: 168
"""

import re
import io
import os 
import time 
import json
import yaml
import pymysql
import numpy as np
import pandas as pd
def load_config():
	"""
	加载配置文件
	return: 配置参数
	"""
	config_file = io.open(r'E:\Dong\code\config_ocr.yml', encoding='utf-8')
	config_ocr = yaml.load(config_file)
	return config_ocr


def conn_mysql_read_table(config_ocr):
	"""
	连接数据库并从数据库中读取相应表格
	Params:
			config_ocr--配置表参数
	return：
			df_table : 读取数据库里的表格列表
	"""
	host = config_ocr['database']['host']
	db_name = config_ocr['database']['db_name']
	port = config_ocr['database']['port']
	user = config_ocr['database']['user']
	passwd = config_ocr['database']['passwd']
	table_name_list = config_ocr['table_name']
	conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd,
						   db=db_name, charset="utf8")
	df_table = list()
	for table_name in table_name_list:
		sql_select = "SELECT * FROM " + table_name
		df_table.append(pd.read_sql(sql_select, conn))
	print("成功连接数据库")
	return df_table


def read_json(file_dir):
	"""
	读取json 文件,后期函数要改，直接调用腾讯API接口
	dir: json文件路径
	return: 
			word_list:票面字符--列表形式
			word_str: 票面字符--字符串
	"""
	count = 0
	dir_list = list()
	word_list_total =list()
	word_str_list = list()
	json_name = list()
	for root, dirs, files in os.walk(file_dir):
		for file in files:
			dir_list.append(root+'\\'+file)
			json_name.append(file)
	for file_dir in dir_list:
		with open(file_dir, 'r', encoding='utf-8') as load_f:
			load_dict = json.load(load_f)
		word_list = list()
		word_str = ''
		if load_dict['errormsg'] == "OK":
			for word_dict in load_dict['items']:
				word = word_dict['itemstring'].encode('iso8859-1').decode('utf-8')
				word_list.append(word)
				word_str += word	
		else:
			count += 1
			word_list = word_str = list()
		word_list_total.append(word_list)
		word_str_list.append(word_str)
	print("共有{}张图片未上传成功".format(count))
	return word_list_total, word_str_list, json_name

def change_bft_to_dict(bft):
	n = len(bft)
	bft_dict = dict()
	for i in range(n):
		bft_dict[bft.iloc[i,1]] = bft.iloc[i,2]
	return bft_dict



def get_fault_tolerant(word, bft_dict):
	if word!='':
		word_ft_expre = ""
		for char in word:
			if char in bft_dict.keys():
				word_ft_expre += "[" + bft_dict[char] + ']'
			else:
				word_ft_expre +='[' + char +']'
	else: 
		return ''
	return word_ft_expre

def get_fault_tolerant_pay2(bft_dict,payer_name='',payer_next='',payee_next=''):
	pay_str = payer_name + payer_next + payee_next
	pay_set = set(pay_str)
	word_ft_expre = "("
	for word in pay_set:
		if word in bft_dict.keys():
			word_ft_expre += "[" + bft_dict[word] + ']|'
		else:
			word_ft_expre += "[" + word + ']|'
	word_ft_expre = word_ft_expre[:-1] + ")"
	return word_ft_expre


def match_bank_ticket_id(df,word_list,bft_dict,btc):
	n = len(word_list)
	ID = "UNK"
	zhong = r"[a-zA-Z\u4e00-\u9fa5]+"
	pattern = re.compile(zhong,re.A)
	mark1 = df['ticket_classifi_mark_1']
	mark1_ft = get_fault_tolerant(mark1, bft_dict)
	mark1_pattern = re.compile(mark1_ft, re.A)
	mark_count = df['ticket_classifi_mark_count']
	btc_curr_i = btc[btc['ticket_classifi_mark_1'] == mark1]
	for i in range(n):
		current_word_list = pattern.findall(word_list[i])
		current_word_str = "".join(current_word_list)
		match_mark1 = mark1_pattern.search(current_word_str)
		if match_mark1:
			if mark_count == 1:	
				ID = df['ID']
				return ID
			else:
				mark2_list = btc_curr_i['ticket_classifi_mark_2'].unique().tolist()
				for j in range(i,n):
					current_word_list = pattern.findall(word_list[j])
					current_word_str = "".join(current_word_list)
					for mark2 in mark2_list:
						mark2_ft = get_fault_tolerant(mark2, bft_dict)
						mark2_pattern = re.compile(mark2_ft, re.A)
						match_mark2 = mark2_pattern.search(current_word_str)
						if match_mark2:
							btc_curr_j = btc_curr_i[btc_curr_i['ticket_classifi_mark_2']==mark2]
							mark_count = btc_curr_j['ticket_classifi_mark_count'].unique().tolist()[0]
							if mark_count == 2:
								ID = btc_curr_j['ID'].tolist()[0]
								return ID
							elif mark_count == 3:
								mark3_list = btc_curr_j['ticket_classifi_mark_3'].unique().tolist()
								for k in range(j,n):
									current_word_list = pattern.findall(word_list[k])
									current_word_str = "".join(current_word_list)
									for mark3 in mark3_list:
										mark3_ft = get_fault_tolerant(mark3, bft_dict)
										mark3_pattern = re.compile(mark3_ft, re.A)
										match_mark3 = mark3_pattern.search(current_word_str)
										if match_mark3:
											btc_curr_k = btc_curr_j[btc_curr_j['ticket_classifi_mark_3']==mark3]
											ID = btc_curr_k['ID'].tolist()[0]
											return ID
	return ID


def ticket_classification(word_list_total, btc, bft_dict):
	num_picture = len(word_list_total)
	bank_ticket_id_list = list()
	for i in range(num_picture):
		word_list = word_list_total[i]
		bank_ticket_id = btc.apply(match_bank_ticket_id,axis=1,args=(word_list,bft_dict,btc))
		bank_ticket_id = bank_ticket_id[bank_ticket_id!='UNK']
		id_list = bank_ticket_id.unique().tolist()
		if not id_list:
			id_list = 'UNK'
		bank_ticket_id_list.append(id_list)
	return bank_ticket_id_list

	
def abstract_key_word(bt_id, word_list_total, btc,bft_dict, json_name):
	"""
	提取票据关键字段
	params: 
		bt_id:
		word_list_total:
		btc:
		bft_dict:
	return:

	"""
	key_word_df = pd.DataFrame(columns=('picture_name','bt_id','payer_name','payee_name','date','word_list_total'))
	key_word_df['picture_name'] = json_name
	key_word_df['bt_id'] = bt_id
	key_word_df['word_list_total'] = word_list_total
	key_word_df['pay_payee'] = key_word_df.apply(abstract_pay_str,axis=1,args=(btc,bft_dict))
	key_word_df['payer_name'] = key_word_df.apply(lambda x:x['pay_payee'][0] if x['pay_payee'] else np.nan,axis=1)
	key_word_df['payee_name'] = key_word_df.apply(lambda x:x['pay_payee'][1] if x['pay_payee'] else np.nan,axis=1)
	key_word_df['date'] = key_word_df.apply(extract_date,axis=1,args=(btc,bft_dict))
	key_word_df['date'] = key_word_df.apply(handle_date,axis=1)
	del key_word_df['word_list_total']
	return key_word_df


def abstract_pay_str(df,btc,bft_dict):
	"""
	提取收付款名称
	paramters：
		bt_id: 银行票据相应id
		bank_receipt_config_table: 银行票据配置表
	return：
		pay_params: 返回收付款名称
	"""
	#payer = payee = "UNK"
	if (df['bt_id'] != 'UNK' and len(df['bt_id'])==1):
		#print('pic name:',df['picture_name'],'df[bt_id]:',df['bt_id'])
		pay_type = btc[btc['ID']==df['bt_id']]['pay_type'].item()
		payer_name = btc[btc['ID']==df['bt_id']]['payer_name'].item()
		payer_next = btc[btc['ID']==df['bt_id']]['payer_next'].item()
		payee_name = btc[btc['ID']==df['bt_id']]['payee_name'].item()
		payee_next = btc[btc['ID']==df['bt_id']]['payee_next'].item()
		word_list = df['word_list_total']
		n = len(df['word_list_total'])

		zhong = r"[\u4e00-\u9fa5]+"
		payer_expre = get_fault_tolerant(payer_name, bft_dict) + "([\u4e00-\u9fa5]*)"
		payee_expre = get_fault_tolerant(payee_name, bft_dict) + "([\u4e00-\u9fa5]*)"
		pattern = re.compile(zhong,re.A)
		pattern_payer = re.compile(payer_expre,re.A)
		pattern_payee = re.compile(payee_expre,re.A)

		payer_next_expre = "(" + get_fault_tolerant(payer_next, bft_dict) + ")"
		payee_next_expre = "(" + get_fault_tolerant(payee_next, bft_dict) + ")"
		pattern_payer_next = re.compile(payer_next_expre,re.A)
		pattern_payee_next = re.compile(payee_next_expre,re.A)
		
		if pay_type == 1:
			payer = 'UNK'
			for i in range(n):
				current_word_list = pattern.findall(word_list[i]) # 每一句word只取中文
				current_word_str = "".join(current_word_list)
				match_payer = pattern_payer.search(current_word_str) # 正则匹配付款人锚点字段
				if match_payer:
					rest_word = match_payer.group(1)
					if len(rest_word)>=1:  # 2/3/6情况之一
						match_payer_next = pattern_payer_next.search(rest_word)
						if match_payer_next: # 匹配到rest_word里有next_word
							index = rest_word.find(match_payer_next.group())
							rest_word = rest_word[0:index]
							if len(rest_word)<1:
								payer = '空'
							else:
								payer = rest_word
						else:
							payer = rest_word
					else:     # 1/4/5情况之一
						next_word_payer = pattern.findall(word_list[i+1])
						next_word_payer = "".join(next_word_payer)
						match_payer_next = pattern_payer_next.search(next_word_payer)
						if match_payer_next:
							index = next_word_payer.find(match_payer_next.group())
							next_word_payer = next_word_payer[0:index]
							if len(next_word_payer)<1:
								payer = '空'
							else:
								payer = next_word_payer
						else:
							payer = next_word_payer
					break
			payee = 'UNK'
			for i in range(n):
				current_word_list = pattern.findall(word_list[i])
				current_word_str = "".join(current_word_list)
				match_payee = pattern_payee.search(current_word_str)
				if match_payee:
					rest_word = match_payee.group(1)
					if len(rest_word)>=1:  
						match_payee_next = pattern_payee_next.search(rest_word)
						if match_payee_next:
							index = rest_word.find(match_payee_next.group())
							rest_word = rest_word[0:index]
							if len(rest_word)<1:
								payee = '空'
							else:
								payee = rest_word
						else:
							payee = rest_word
					else:
						next_word_payee = pattern.findall(word_list[i+1])
						next_word_payee = "".join(next_word_payee)
						match_payee_next = pattern_payee_next.search(next_word_payee)
						if match_payee_next:
							index = next_word_payee.find(match_payee_next.group())
							next_word_payee = next_word_payee[0:index]
							if len(next_word_payee)<1:
								payee = '空'
							else:
								payee = next_word_payee
						else:
							payee = next_word_payee
					break
			if payer == '':
				payer = '空'
			if payee == '':
				payee = '空'
			return payer,payee

		elif pay_type == 2:
			pay_next = [payer_next, payee_next]
			flag = 0
			pay = ['UNK','UNK']
			word_list2 = list()
			pay2_expre = get_fault_tolerant_pay2(bft_dict,payer_name,payer_next,payee_next)
			pay2_pattern = re.compile(pay2_expre)
			for i in range(n):
				current_word_list = pattern.findall(word_list[i]) # 每一句word只取中文
				current_word_str = "".join(current_word_list)
				con1 = len(current_word_str)<2 
				con2 = pay2_pattern.search(current_word_str)
				if con1 and (not con2):
					pass
				else:
					word_list2.append(current_word_str)
			
			for i in range(len(word_list2)):
				pay2_match = pattern_payer.search(word_list2[i])
				if pay2_match and flag<2:
					if len(word_list2[i])>2:
						m = word_list2[i].find(payer_name)
						pay[flag] = word_list2[i][m+len(payer_name):]
						flag += 1
					else:
						pay_next_expre = get_fault_tolerant_pay2(bft_dict,payer_next=pay_next[flag])
						pay_next_pattern = re.compile(pay_next_expre)
						if i != len(word_list2)-1:
							pay_next_match = pay_next_pattern.search(word_list2[i+1])		
							if pay_next_match and pay_next_match.end()<5:
								pay[flag] = "空"
							else:
								pay[flag] = word_list2[i+1]
							flag +=1
				elif flag>=2:
					break
			return pay[0],pay[1]

		elif pay_type == 3:
			payer = "银行"
			payee = 'UNK'
			for i in range(n):
				current_word_list = pattern.findall(word_list[i])
				current_word_str = "".join(current_word_list)
				match_payee = pattern_payee.search(current_word_str)
				if match_payee:
					rest_word = match_payee.group(1)
					if len(rest_word)>=1:  
						match_payee_next = pattern_payee_next.search(rest_word)
						if match_payee_next:
							index = rest_word.find(match_payee_next.group())
							rest_word = rest_word[0:index]
							if len(rest_word)<1:
								payee = '空'
							else:
								payee = rest_word
						else:
							payee = rest_word
					else:
						next_word_payee = pattern.findall(word_list[i+1])
						next_word_payee = "".join(next_word_payee)
						match_payee_next = pattern_payee_next.search(next_word_payee)
						if match_payee_next:
							index = next_word_payee.find(match_payee_next.group())
							next_word_payee = next_word_payee[0:index]
							if len(next_word_payee)<1:
								payee = '空'
							else:
								payee = next_word_payee
						else:
							payee = next_word_payee
					break
			if payer == '':
				payer = '空'
			if payee == '':
				payee = '空'
			return payer, payee
		elif pay_type == 4:
			payee = "银行"
			payer = 'UNK'
			for i in range(n):
				current_word_list = pattern.findall(word_list[i]) # 每一句word只取中文
				current_word_str = "".join(current_word_list)
				match_payer = pattern_payer.search(current_word_str) # 正则匹配付款人锚点字段
				if match_payer:
					rest_word = match_payer.group(1)
					if len(rest_word)>=1:  # 2/3/6情况之一
						match_payer_next = pattern_payer_next.search(rest_word)
						if match_payer_next: # 匹配到rest_word里有next_word
							index = rest_word.find(match_payer_next.group())
							rest_word = rest_word[0:index]
							if len(rest_word)<1:
								payer = '空'
							else:
								payer = rest_word
						else:
							payer = rest_word
					else:     # 1/4/5情况之一
						next_word_payer = pattern.findall(word_list[i+1])
						next_word_payer = "".join(next_word_payer)
						match_payer_next = pattern_payer_next.search(next_word_payer)
						if match_payer_next:
							index = next_word_payer.find(match_payer_next.group())
							next_word_payer = next_word_payer[0:index]
							if len(next_word_payer)<1:
								payer = '空'
							else:
								payer = next_word_payer
						else:
							payer = next_word_payer
					break
			if payer == '':
				payer = '空'
			if payee == '':
				payee = '空'
			return payer, payee
	else:
		return df['bt_id'],df['bt_id']


def extract_date_from_all_words(word_list):
	pattern1 = re.compile(r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*([01iC][SiCO\d])[\u4e00-\u9fa5]*([0123iCO][SiCO\d])[\u4e00-\u9fa5]*')
	pattern2 = re.compile(r'[\u4e00-\u9fa5\da-zA-Z]+')
	for item in word_list:
		curr_list = pattern2.findall(item)
		curr_string = ''.join(curr_list)
		if len(curr_string) <= 12:  #2000张只有几张张日期的len>12的
			match = pattern1.search(curr_string)
			if match:
				return match.groups() 
	else:
		return 'UNK'

def extract_date(df, btc, bft_dict): 
	"""
	"""
	if df['bt_id']!= 'UNK':
		date_name = btc[btc['ID']==df['bt_id']]['date_name'].item()
		date_type = btc[btc['ID']==df['bt_id']]['date_type'].item()
		word_list = df['word_list_total']
		if date_type == 1:                             # 为锚点生成正则表达式 
			compres1 = get_fault_tolerant(date_name, bft_dict)
			if date_name == '日期':
				compres1 = '^' + compres1
			#print('picture_name:',df['picture_name'],' compres1:',compres1)
			# recognize (each char of date_name or \s) any time (2[0C][\dC][\dC])[\s.\-/\u4e00-\u9fa5]*([0C1][\dC])[\s.\-/\u4e00-\u9fa5]*([0123C][\dC])[\s.\-/\u4e00-\u9fa5]*
			pattern1 = re.compile(compres1)
			pattern = re.compile(r'[\u4e00-\u9fa5\da-zA-Z]+')
			for index, item in enumerate(word_list):   # traversal 1th-kth words
				curr_list = pattern.findall(item)      # item只保留中文和数字
				curr_string = ''.join(curr_list)
				#print('picture_name:',df['picture_name'],'curr_string:',curr_string)
				match1 = pattern1.search(curr_string)  # 搜索最靠前且符合pattern1的字符
				if match1:                             # 如果锚点匹配上
					pattern2 = re.compile(
						r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*([01iC][SiCO\d])[\u4e00-\u9fa5]*([0123iCO][SiCO\d])[\u4e00-\u9fa5]*')
					match2 = pattern2.search(curr_string)
					if match2:                         # 如果在锚点所在word匹配到日期
						return match2.groups()
					else:                              # 没在锚点所在word匹配到日期
						nextWord = word_list[index+1]
						next_list = pattern.findall(nextWord)
						next_string = ''.join(next_list)
						pattern3 = re.compile(
							r'(2[0C][SiCO\d][SiCO\d])[\u4e00-\u9fa5]*([01iC][SiCO\d])[\u4e00-\u9fa5]*([0123iCO][SiCO\d])[\u4e00-\u9fa5]*')
						match3 = pattern3.search(next_string)
						if match3:
							return match3.groups()   # 这个word没匹配到则会再次进入for循环接着下面的查找
				else:
					continue
			else:  # 遍历完所有word没匹配到date_name，说明套打打偏了，直接按date_type == 2的方式匹配
				return extract_date_from_all_words(word_list)

		elif(date_type == 2):
			return extract_date_from_all_words(word_list)

		else:
			return 'UNK'
	return "UNK"

def handle_date(df):
	date = df['date']
	if date != 'UNK':
		date_list = list(date)
		for i in range(3):
			date_list[i] = date_list[i].replace("S","5")
			date_list[i] = date_list[i].replace("i","1")
			date_list[i] = date_list[i].replace("C","0")
			date_list[i] = date_list[i].replace("O","0")
		if date_list[0]>'2020':
			return 'UNK'
		if date_list[1]>'12':
			return 'UNK'
		if date_list[2]>'31':
			return 'UNK'
		date = tuple(date_list)
	return date

##def main():
#	# 1. 加载配置表
#	config_ocr = load_config()
#	# 2. 获得数据库数据
#	df_table = conn_mysql_read_table(config_ocr)
#	bft, btc = df_table
#	# 3. 将容错表数据变为字典
#	bft_dict = change_bft_to_dict(bft)
#	# 4. 上传图片至腾讯优图返回json文件
#	
#	# 5. 读取json文件处理成字符串
#	json_dir = config_ocr['dir']
#	word_list_total, word_str_list, json_name = read_json(json_dir)
#	# 6. 票据分类，返回票据ID
#	bt_id = ticket_classification(word_list_total, btc, bft_dict)
#	# 7. 提取关键字段
#	df = abstract_key_word(bt_id, word_list_total, btc, bft_dict, json_name)
#	print(df)
#	#if word_list:
#	#	payer, payee = abstract_pay_str(word_list, word_str)
#	#	print(payer, payee,sep='\n')


if __name__ == '__main__':
	s = time.time()
	#main()
 
 	# 1. 加载配置表
	config_ocr = load_config()
	# 2. 获得数据库数据
	df_table = conn_mysql_read_table(config_ocr)
	bft, btc = df_table
	# 3. 将容错表数据变为字典
	bft_dict = change_bft_to_dict(bft)
	# 4. 上传图片至腾讯优图返回json文件
	
	# 5. 读取json文件处理成字符串
	json_dir = config_ocr['dir']
	word_list_total, word_str_list, json_name = read_json(json_dir)
	# 6. 票据分类，返回票据ID
	bt_id = ticket_classification(word_list_total, btc, bft_dict)
	# 7. 提取关键字段
	df = abstract_key_word(bt_id, word_list_total, btc, bft_dict, json_name)
	#print(df)
	#if word_list:
	#	payer, payee = abstract_pay_str(word_list, word_str)
	#	print(payer, payee,sep='\n')
 
	e = time.time()
	print(int(e-s))
