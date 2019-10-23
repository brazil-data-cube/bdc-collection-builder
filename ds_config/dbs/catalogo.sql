-- phpMyAdmin SQL Dump
-- version 4.9.0.1
-- https://www.phpmyadmin.net/
--
-- Host: ds_db
-- Tempo de geração: 23/10/2019 às 11:39
-- Versão do servidor: 5.6.45
-- Versão do PHP: 7.2.19

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Banco de dados: `catalogo`
--

-- --------------------------------------------------------

--
-- Estrutura para tabela `Dataset`
--

CREATE TABLE `Dataset` (
  `Id` int(11) NOT NULL,
  `Name` varchar(50) NOT NULL,
  `Description` varchar(512) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura para tabela `Product`
--

CREATE TABLE `Product` (
  `Id` int(11) NOT NULL,
  `Dataset` varchar(50) NOT NULL,
  `Type` varchar(20) NOT NULL,
  `ProcessingDate` datetime DEFAULT NULL,
  `GeometricProcessing` varchar(20) DEFAULT NULL,
  `RadiometricProcessing` varchar(20) DEFAULT NULL,
  `SceneId` varchar(64) DEFAULT NULL,
  `Band` varchar(20) DEFAULT NULL,
  `Resolution` float NOT NULL,
  `Filename` varchar(255) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura para tabela `Qlook`
--

CREATE TABLE `Qlook` (
  `SceneId` varchar(64) NOT NULL DEFAULT '',
  `QLfilename` varchar(255) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura para tabela `Request`
--

CREATE TABLE `Request` (
  `ReqId` int(11) NOT NULL,
  `UserId` varchar(254) NOT NULL,
  `ReqDate` datetime NOT NULL,
  `StatusDate` datetime NOT NULL,
  `PayDate` datetime DEFAULT NULL,
  `DelDate` datetime DEFAULT NULL,
  `Priority` int(11) NOT NULL,
  `Operator` varchar(20) NOT NULL,
  `addressId` int(11) NOT NULL,
  `Ip` varchar(20) DEFAULT NULL,
  `Country` varchar(50) DEFAULT NULL,
  `Language` char(2) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura para tabela `Scene`
--

CREATE TABLE `Scene` (
  `SceneId` varchar(64) NOT NULL DEFAULT '',
  `IdRunMode` int(11) DEFAULT NULL,
  `Satellite` varchar(50) DEFAULT NULL,
  `Sensor` varchar(6) NOT NULL,
  `Path` varchar(11) DEFAULT NULL,
  `Row` varchar(11) DEFAULT NULL,
  `Date` date DEFAULT NULL,
  `Orbit` int(11) NOT NULL,
  `CenterLatitude` float DEFAULT NULL,
  `CenterLongitude` float DEFAULT NULL,
  `TL_Latitude` float DEFAULT NULL,
  `TL_Longitude` float DEFAULT NULL,
  `BR_Latitude` float DEFAULT NULL,
  `BR_Longitude` float DEFAULT NULL,
  `TR_Latitude` float DEFAULT NULL,
  `TR_Longitude` float DEFAULT NULL,
  `BL_Latitude` float DEFAULT NULL,
  `BL_Longitude` float DEFAULT NULL,
  `CenterTime` double DEFAULT NULL,
  `StartTime` double DEFAULT NULL,
  `StopTime` double DEFAULT NULL,
  `ImageOrientation` float DEFAULT NULL,
  `SyncLosses` int(11) DEFAULT NULL,
  `NumMissSwath` int(11) DEFAULT NULL,
  `PerMissSwath` float DEFAULT NULL,
  `BitSlips` int(11) DEFAULT NULL,
  `CloudCoverQ1` int(11) DEFAULT NULL,
  `CloudCoverQ2` int(11) DEFAULT NULL,
  `CloudCoverQ3` int(11) DEFAULT NULL,
  `CloudCoverQ4` int(11) DEFAULT NULL,
  `CloudCoverMethod` char(1) DEFAULT NULL,
  `Grade` float DEFAULT NULL,
  `IngestDate` datetime DEFAULT NULL,
  `Deleted` smallint(6) NOT NULL,
  `Dataset` varchar(50) DEFAULT NULL,
  `ExportDate` datetime DEFAULT NULL,
  `AuxPath` varchar(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Índices de tabelas apagadas
--

--
-- Índices de tabela `Dataset`
--
ALTER TABLE `Dataset`
  ADD PRIMARY KEY (`Id`);

--
-- Índices de tabela `Product`
--
ALTER TABLE `Product`
  ADD PRIMARY KEY (`Id`),
  ADD KEY `Product_idx4` (`GeometricProcessing`),
  ADD KEY `Product_idx3` (`Type`),
  ADD KEY `Product_idx1` (`SceneId`),
  ADD KEY `Product_idx6` (`Band`),
  ADD KEY `Product_idx5` (`RadiometricProcessing`),
  ADD KEY `Product_idx2` (`Dataset`),
  ADD KEY `Product_idx7` (`ProcessingDate`);

--
-- Índices de tabela `Qlook`
--
ALTER TABLE `Qlook`
  ADD PRIMARY KEY (`SceneId`),
  ADD UNIQUE KEY `Filename` (`QLfilename`),
  ADD KEY `Product_idx1` (`SceneId`);

--
-- Índices de tabela `Request`
--
ALTER TABLE `Request`
  ADD PRIMARY KEY (`ReqId`);

--
-- Índices de tabela `Scene`
--
ALTER TABLE `Scene`
  ADD PRIMARY KEY (`SceneId`),
  ADD KEY `Scene_idx4` (`Row`),
  ADD KEY `Scene_idx2` (`Date`),
  ADD KEY `Scene_idx3` (`Path`),
  ADD KEY `Scene_idx1` (`Satellite`,`Sensor`),
  ADD KEY `Scene_idx5` (`Sensor`),
  ADD KEY `Scene_idx6` (`CloudCoverQ1`,`CloudCoverQ2`,`CloudCoverQ3`,`CloudCoverQ4`);

--
-- AUTO_INCREMENT de tabelas apagadas
--

--
-- AUTO_INCREMENT de tabela `Dataset`
--
ALTER TABLE `Dataset`
  MODIFY `Id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `Product`
--
ALTER TABLE `Product`
  MODIFY `Id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `Request`
--
ALTER TABLE `Request`
  MODIFY `ReqId` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
