import logging

#创建logger对象，一个程序就有一个logger
logger = logging.getLogger('test-log')
logger.setLevel(logging.DEBUG)  ##注意，这个是全局默认配置，不过目前是谁的过滤高听谁的

#创建一个控制台输出并且设定他的输出等级
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

#创建一个日志文件输出并且设定他的输出等级
fh = logging.FileHandler("access.log")
fh.setLevel(logging.WARNING)

#做一个日志输出的格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#把日志的输出格式扔给控制台输出和文件输出
ch.setFormatter(formatter)
fh.setFormatter(formatter)

#文件和控制台输出一人用一个logger
logger.addHandler(ch)
logger.addHandler(fh)

#程序输出日志
logger.debug('debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')