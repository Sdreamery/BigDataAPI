/*
MySQL Data Transfer
Source Host: 192.168.179.4
Source Database: traffic
Target Host: 192.168.179.4
Target Database: traffic
Date: 2017/10/18 17:00:20
*/

SET FOREIGN_KEY_CHECKS=0;
-- ----------------------------
-- Table structure for area_info
-- ----------------------------
CREATE TABLE `area_info` (
  `area_id` varchar(255) DEFAULT NULL,
  `area_name` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for car_track
-- ----------------------------
CREATE TABLE `car_track` (
  `task_id` varchar(255) DEFAULT NULL,
  `date` varchar(255) DEFAULT NULL,
  `car` varchar(255) DEFAULT NULL,
  `car_track` text
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for monitor_range_time_car
-- ----------------------------
CREATE TABLE `monitor_range_time_car` (
  `task_id` varchar(255) DEFAULT NULL,
  `monitor_id` varchar(255) DEFAULT NULL,
  `range_time` varchar(255) DEFAULT NULL,
  `cars` text
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for monitor_state
-- ----------------------------
CREATE TABLE `monitor_state` (
  `taskId` varchar(255) DEFAULT NULL,
  `noraml_monitor_count` varchar(255) DEFAULT NULL,
  `normal_camera_count` varchar(255) DEFAULT NULL,
  `abnormal_monitor_count` varchar(255) DEFAULT NULL,
  `abnormal_camera_count` varchar(255) DEFAULT NULL,
  `abnormal_monitor_camera_infos` longtext
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for random_extract_car
-- ----------------------------
CREATE TABLE `random_extract_car` (
  `task_id` varchar(255) DEFAULT NULL,
  `car_info` varchar(255) DEFAULT NULL,
  `date_d` varchar(255) DEFAULT NULL,
  `date_hour` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for random_extract_car_detail_info
-- ----------------------------
CREATE TABLE `random_extract_car_detail_info` (
  `task_id` varchar(255) DEFAULT NULL,
  `date` varchar(255) DEFAULT NULL,
  `monitor_id` varchar(255) DEFAULT NULL,
  `camera_id` varchar(255) DEFAULT NULL,
  `car` varchar(255) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `speed` varchar(255) DEFAULT NULL,
  `road_id` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for task
-- ----------------------------
CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `task_name` varchar(255) DEFAULT NULL COMMENT '任务名称',
  `create_time` varchar(255) DEFAULT NULL COMMENT '任务创建时间',
  `start_time` varchar(255) DEFAULT NULL COMMENT '任务执行时间',
  `finish_time` varchar(255) DEFAULT NULL COMMENT '任务结束时间',
  `task_type` varchar(255) DEFAULT NULL COMMENT '任务类型     一个模块一个任务类型',
  `task_status` varchar(255) DEFAULT NULL COMMENT '任务状态  创建-执行-结束 ',
  `task_param` text COMMENT '任务参数  json',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for top10_speed_detail
-- ----------------------------
CREATE TABLE `top10_speed_detail` (
  `task_id` varchar(255) DEFAULT NULL,
  `date` varchar(255) DEFAULT NULL,
  `monitor_id` varchar(255) DEFAULT NULL,
  `camera_id` varchar(255) DEFAULT NULL,
  `car` varchar(255) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `speed` varchar(255) DEFAULT NULL,
  `road_id` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for topn_monitor_car_count
-- ----------------------------
CREATE TABLE `topn_monitor_car_count` (
  `task_id` varchar(11) DEFAULT NULL,
  `monitor_id` varchar(11) DEFAULT NULL,
  `carCount` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for topn_monitor_detail_info
-- ----------------------------
CREATE TABLE `topn_monitor_detail_info` (
  `task_id` varchar(255) DEFAULT NULL,
  `date` varchar(255) DEFAULT NULL,
  `monitor_id` varchar(255) DEFAULT NULL,
  `camera_id` varchar(255) DEFAULT NULL,
  `car` varchar(255) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `speed` varchar(255) DEFAULT NULL,
  `road_id` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records 
-- ----------------------------

INSERT INTO `area_info` VALUES ('01', '黄浦区');
INSERT INTO `area_info` VALUES ('02', '徐汇区');
INSERT INTO `area_info` VALUES ('03', '长宁区');
INSERT INTO `area_info` VALUES ('04', '松江区');
INSERT INTO `area_info` VALUES ('05', '浦东区');
INSERT INTO `area_info` VALUES ('06', '金山区');
INSERT INTO `area_info` VALUES ('07', '嘉定区');
INSERT INTO `area_info` VALUES ('08', '青浦区');
INSERT INTO `task` VALUES ('1', '卡口流量监测', null, '', '', null, null, '{\"startDate\":[\"2018-07-02\"],\"endDate\":[\"2018-07-02\"],\"topNum\":[\"5\"],\"areaName\":[\"黄浦区\"]}');
INSERT INTO `task` VALUES ('2', '随机抽取N个车辆信息', null, null, null, null, null, '{\"startDate\":[\"2018-07-02\"],\"endDate\":[\"2018-07-02\"],\"extractNum\":[\"100\"]}');
INSERT INTO `task` VALUES ('3', '跟车分析', null, null, null, null, null, '{\"startDate\":[\"2018-07-02\"],\"endDate\":[\"2018-07-02\"],\"cars\":[\"沪I42152,沪Q18277,沪K10100,沪R24874,沪N63229,沪E25462,沪W43404,京J13254,鲁G65763,鲁R55733,京L32167,京R54122,京K44557,京W41927,京S90923,京D86196,京W63299,沪N19518,京B47292,京A11951,沪D71306,沪D39243,京G44724,京E05123,京Y03722,京O28098,鲁Y63080,深N55336,京G89927,京Z29402\"]}');
INSERT INTO `task` VALUES ('4', '各个区域topN的车流量', null, null, null, null, null, '{\"startDate\":[\"2018-07-02\"],\"endDate\":[\"2018-07-02\"]}');
INSERT INTO `task` VALUES ('5', '道路转化率', null, null, null, null, null, '{\"startDate\":[\"2018-07-02\"],\"endDate\":[\"2018-07-02\"],\"roadFlow\":[\"0001,0002,0003,0004,0005\"]}');
