-- phpMyAdmin SQL Dump
-- version 4.9.0.1
-- https://www.phpmyadmin.net/
--
-- Host: ds_db
-- Tempo de geração: 23-Out-2019 às 11:37
-- Versão do servidor: 10.4.7-MariaDB-1:10.4.7+maria~bionic
-- versão do PHP: 7.2.19

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Banco de dados: `datastorm`
--

-- --------------------------------------------------------

--
-- Estrutura da tabela `activities`
--

CREATE TABLE `activities` (
  `id` bigint(20) NOT NULL,
  `app` varchar(64) NOT NULL,
  `datacube` varchar(32) DEFAULT NULL,
  `tileid` varchar(16) DEFAULT NULL,
  `start` date NOT NULL,
  `end` date NOT NULL,
  `ttable` varchar(16) DEFAULT NULL,
  `tid` bigint(20) DEFAULT NULL,
  `tsceneid` varchar(64) DEFAULT NULL,
  `band` varchar(16) DEFAULT NULL,
  `priority` int(11) DEFAULT NULL,
  `status` varchar(16) DEFAULT NULL,
  `pstart` datetime DEFAULT NULL,
  `pend` datetime DEFAULT NULL,
  `elapsed` time DEFAULT NULL,
  `retcode` int(11) DEFAULT NULL,
  `message` varchar(512) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `datacubes`
--

CREATE TABLE `datacubes` (
  `id` bigint(20) NOT NULL,
  `datacube` varchar(48) NOT NULL,
  `wrs` varchar(16) NOT NULL,
  `tschema` varchar(16) NOT NULL,
  `step` int(11) NOT NULL,
  `satsen` varchar(32) NOT NULL,
  `bands` varchar(128) NOT NULL,
  `quicklook` varchar(64) NOT NULL DEFAULT 'swir2,nir,red',
  `start` date DEFAULT NULL,
  `end` date DEFAULT NULL,
  `resx` float NOT NULL,
  `resy` float NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `mosaics`
--

CREATE TABLE `mosaics` (
  `id` bigint(20) NOT NULL,
  `datacube` varchar(32) NOT NULL,
  `tileid` varchar(16) NOT NULL,
  `start` date NOT NULL,
  `end` date NOT NULL,
  `numcol` int(11) NOT NULL,
  `numlin` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `products`
--

CREATE TABLE `products` (
  `id` bigint(20) NOT NULL,
  `datacube` varchar(48) NOT NULL,
  `tileid` varchar(16) NOT NULL,
  `start` date NOT NULL,
  `end` date NOT NULL,
  `type` varchar(16) NOT NULL DEFAULT 'SCENE',
  `sceneid` varchar(64) NOT NULL,
  `band` varchar(16) NOT NULL,
  `cloud` float NOT NULL,
  `processingdate` datetime DEFAULT NULL,
  `TL_Latitude` float DEFAULT NULL,
  `TL_Longitude` float DEFAULT NULL,
  `BR_Latitude` float DEFAULT NULL,
  `BR_Longitude` float DEFAULT NULL,
  `TR_Latitude` float DEFAULT NULL,
  `TR_Longitude` float DEFAULT NULL,
  `BL_Latitude` float DEFAULT NULL,
  `BL_Longitude` float DEFAULT NULL,
  `filename` varchar(255) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `qlook`
--

CREATE TABLE `qlook` (
  `id` bigint(20) NOT NULL,
  `datacube` varchar(48) NOT NULL,
  `tileid` varchar(16) NOT NULL,
  `start` date NOT NULL,
  `end` date NOT NULL,
  `sceneid` varchar(64) NOT NULL,
  `qlookfile` varchar(256) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `scenes`
--

CREATE TABLE `scenes` (
  `id` bigint(20) NOT NULL,
  `datacube` varchar(32) NOT NULL,
  `tileid` varchar(16) NOT NULL,
  `start` date NOT NULL,
  `end` date NOT NULL,
  `type` varchar(16) NOT NULL DEFAULT 'SCENE',
  `dataset` varchar(16) NOT NULL,
  `sceneid` varchar(64) NOT NULL,
  `band` varchar(16) NOT NULL,
  `pathrow` varchar(16) NOT NULL,
  `date` date NOT NULL,
  `cloud` float NOT NULL,
  `resolution` float NOT NULL,
  `cloudratio` float NOT NULL,
  `clearratio` float NOT NULL,
  `efficacy` float NOT NULL,
  `link` varchar(256) NOT NULL,
  `file` varchar(256) DEFAULT NULL,
  `warped` varchar(256) NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Estrutura da tabela `wrs`
--

CREATE TABLE `wrs` (
  `name` varchar(16) NOT NULL,
  `path` int(11) NOT NULL,
  `row` int(11) NOT NULL,
  `tileid` varchar(16) NOT NULL,
  `xmin` float NOT NULL,
  `xmax` float NOT NULL,
  `ymin` float NOT NULL,
  `ymax` float NOT NULL,
  `lonmin` float NOT NULL,
  `lonmax` float NOT NULL,
  `latmin` float NOT NULL,
  `latmax` float NOT NULL,
  `srs` varchar(128) NOT NULL,
  `geom` varchar(1024) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Índices para tabelas despejadas
--

--
-- Índices para tabela `activities`
--
ALTER TABLE `activities`
  ADD UNIQUE KEY `id` (`id`),
  ADD KEY `sceneid` (`tsceneid`,`band`) USING BTREE;

--
-- Índices para tabela `datacubes`
--
ALTER TABLE `datacubes`
  ADD PRIMARY KEY (`id`);

--
-- Índices para tabela `mosaics`
--
ALTER TABLE `mosaics`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `general` (`datacube`,`tileid`,`start`,`end`);

--
-- Índices para tabela `products`
--
ALTER TABLE `products`
  ADD PRIMARY KEY (`id`),
  ADD KEY `general` (`type`,`datacube`,`tileid`,`start`,`end`);

--
-- Índices para tabela `qlook`
--
ALTER TABLE `qlook`
  ADD PRIMARY KEY (`id`),
  ADD KEY `sceneid` (`sceneid`);

--
-- Índices para tabela `scenes`
--
ALTER TABLE `scenes`
  ADD PRIMARY KEY (`id`),
  ADD KEY `general` (`datacube`,`tileid`,`start`,`end`,`band`) USING BTREE,
  ADD KEY `enab` (`enabled`);

--
-- Índices para tabela `wrs`
--
ALTER TABLE `wrs`
  ADD UNIQUE KEY `geo` (`lonmin`,`lonmax`,`latmin`,`latmax`),
  ADD UNIQUE KEY `npr` (`name`,`tileid`);

--
-- AUTO_INCREMENT de tabelas despejadas
--

--
-- AUTO_INCREMENT de tabela `activities`
--
ALTER TABLE `activities`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `datacubes`
--
ALTER TABLE `datacubes`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `mosaics`
--
ALTER TABLE `mosaics`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `products`
--
ALTER TABLE `products`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `qlook`
--
ALTER TABLE `qlook`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de tabela `scenes`
--
ALTER TABLE `scenes`
  MODIFY `id` bigint(20) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
