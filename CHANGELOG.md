Release Notes
---

## [0.15.0](https://github.com/AlertaDengue/PySUS/compare/0.14.3...0.15.0) (2024-12-03)

### Features

* **package:** bump version to 0.15.0 ([#216](https://github.com/AlertaDengue/PySUS/issues/216)) ([8ed64be](https://github.com/AlertaDengue/PySUS/commit/8ed64be381531306152edf0aa7c57711fff4b293))

## [0.14.3](https://github.com/AlertaDengue/PySUS/compare/0.14.2...0.14.3) (2024-07-31)

### Bug Fixes

* **release:** bump version on PyPI to 0.14.3 ([#204](https://github.com/AlertaDengue/PySUS/issues/204)) ([7122019](https://github.com/AlertaDengue/PySUS/commit/7122019a5278976bb659768360e141ba405b2c5d))

## [0.14.2](https://github.com/AlertaDengue/PySUS/compare/0.14.1...0.14.2) (2024-07-31)

### Bug Fixes

* **release:** bump version on PyPI ([#203](https://github.com/AlertaDengue/PySUS/issues/203)) ([7d800fa](https://github.com/AlertaDengue/PySUS/commit/7d800fa4399854f161e6d4a1a527299e5b6e3d70))

## [0.14.1](https://github.com/AlertaDengue/PySUS/compare/0.14.0...0.14.1) (2024-07-16)

### Bug Fixes

* **SIA:** handle file_N.dbf files on SIA database ([#200](https://github.com/AlertaDengue/PySUS/issues/200)) ([997b419](https://github.com/AlertaDengue/PySUS/commit/997b41939bc80ce371fb433fb03b9eed18e358ea))

## [0.14.0](https://github.com/AlertaDengue/PySUS/compare/0.13.1...0.14.0) (2024-04-16)


### Features

* **IBGE:** continue work of AlertaDengue [#192](https://github.com/AlertaDengue/PySUS/issues/192) ([#193](https://github.com/AlertaDengue/PySUS/issues/193)) ([b3540d7](https://github.com/AlertaDengue/PySUS/commit/b3540d71b528b2eb2c57f3521c1032a341d8a8ef))

## [0.13.1](https://github.com/AlertaDengue/PySUS/compare/0.13.0...0.13.1) (2024-01-12)


### Bug Fixes

* **release:** update node version on release.yaml ([#186](https://github.com/AlertaDengue/PySUS/issues/186)) ([0313b79](https://github.com/AlertaDengue/PySUS/commit/0313b796cb5b04e60a7adcca40d6a5d92af95444))

## [0.13.0](https://github.com/AlertaDengue/PySUS/compare/0.12.0...0.13.0) (2023-12-21)


### Features

* **PNI:** include PNI Database ([#181](https://github.com/AlertaDengue/PySUS/issues/181)) ([67fca92](https://github.com/AlertaDengue/PySUS/commit/67fca920d744f82dc760bf6217746290d15d9c96))


### Bug Fixes

* Replace applymap with map for pd.DataFrame ([#180](https://github.com/AlertaDengue/PySUS/issues/180)) ([357b7ca](https://github.com/AlertaDengue/PySUS/commit/357b7ca5498dd7030bcf31c42179ef6a197b8f38))

## [0.12.0](https://github.com/AlertaDengue/PySUS/compare/0.11.0...0.12.0) (2023-12-05)


### Features

* **deps:** Update Python version constraints to support 3.10 and 3.11 ([#179](https://github.com/AlertaDengue/PySUS/issues/179)) ([ba8b7e5](https://github.com/AlertaDengue/PySUS/commit/ba8b7e50b2dade66e6d0e4b7baaa2962f1217971))

## [0.11.0](https://github.com/AlertaDengue/PySUS/compare/0.10.3...0.11.0) (2023-12-05)


### Features

* Introduce 'data_sources' function to list supported databases ([#177](https://github.com/AlertaDengue/PySUS/issues/177)) ([5d4e3b2](https://github.com/AlertaDengue/PySUS/commit/5d4e3b2964d48e40b1fe48f3d7f80c951c9c654b))


### Bug Fixes

* Separate dependencies into groups for Poetry installation ([#175](https://github.com/AlertaDengue/PySUS/issues/175)) ([4e707bf](https://github.com/AlertaDengue/PySUS/commit/4e707bfb6dba22ea67d26e4b811049562eed2930))

## [0.10.3](https://github.com/AlertaDengue/PySUS/compare/0.10.2...0.10.3) (2023-10-19)


### Bug Fixes

* **FTP:** remove DBF from content if DBC is present ([#168](https://github.com/AlertaDengue/PySUS/issues/168)) ([82da723](https://github.com/AlertaDengue/PySUS/commit/82da723ce35d081899bbd3c90a353beedae6f2a1))

## [0.10.2](https://github.com/AlertaDengue/PySUS/compare/0.10.1...0.10.2) (2023-09-25)


### Bug Fixes

* **online_data:** remove plural variables from database calls on online_data ([#163](https://github.com/AlertaDengue/PySUS/issues/163)) ([6ecd960](https://github.com/AlertaDengue/PySUS/commit/6ecd96046799b90329d11298f8aa17f8d86d25c4))

## [0.10.1](https://github.com/AlertaDengue/PySUS/compare/0.10.0...0.10.1) (2023-09-21)


### Bug Fixes

* **sia:** handles files with final 'a', 'b' and 'c' ([#162](https://github.com/AlertaDengue/PySUS/issues/162)) ([2d8cfb2](https://github.com/AlertaDengue/PySUS/commit/2d8cfb27aae2a50ac836804ceacb88db6143134e))

## [0.10.0](https://github.com/AlertaDengue/PySUS/compare/0.9.4...0.10.0) (2023-09-19)


### Features

* **databases:** create CACHE structure to ftp Directories & add CNES database ([#152](https://github.com/AlertaDengue/PySUS/issues/152)) ([b99dd38](https://github.com/AlertaDengue/PySUS/commit/b99dd38a2bfbf286bc3061af7f9b1f3a1e40627d))
* **pbar:** include a progress bar to download and parsing data ([8cd691c](https://github.com/AlertaDengue/PySUS/commit/8cd691cc875fb46cb4aadf59e0cfb46670f4dab0))
* **struc:** database modularization and code improvement ([#137](https://github.com/AlertaDengue/PySUS/issues/137)) ([d7e6d27](https://github.com/AlertaDengue/PySUS/commit/d7e6d271838ed442e137ca788d8ade302299fc27))


### Bug Fixes

* **pyreaddbc:** update pyreaddbc to fix dbc parsing bug ([#153](https://github.com/AlertaDengue/PySUS/issues/153)) ([4c8315a](https://github.com/AlertaDengue/PySUS/commit/4c8315abbe7747f1d2f401659095431d6f8be439))
* **release:** include main branch on workflow_dispatch ([#155](https://github.com/AlertaDengue/PySUS/issues/155)) ([8f9367d](https://github.com/AlertaDengue/PySUS/commit/8f9367dbf1e3d6ade7337bc4d66a79ec29e993a1))
* **release:** update .releaserc.json ([#156](https://github.com/AlertaDengue/PySUS/issues/156)) ([f9fc7f4](https://github.com/AlertaDengue/PySUS/commit/f9fc7f4de36cc4c8a810e342f362f013bf72cacf))
* **release:** update branch from master to main on release file ([#154](https://github.com/AlertaDengue/PySUS/issues/154)) ([4600137](https://github.com/AlertaDengue/PySUS/commit/46001375393a7513b1fe816b7df3c55c2941eda0))

## [0.9.4](https://github.com/AlertaDengue/PySUS/compare/0.9.3...0.9.4) (2023-07-31)


### Bug Fixes

* **dependencies:** minor dependencies fix ([#142](https://github.com/AlertaDengue/PySUS/issues/142)) ([acbf6e0](https://github.com/AlertaDengue/PySUS/commit/acbf6e0f5dde21348723f474761f687090256258))

## [0.9.3](https://github.com/AlertaDengue/PySUS/compare/0.9.2...0.9.3) (2023-06-06)


### Bug Fixes

* **docs:** pt and pt_BR build ([12b21de](https://github.com/AlertaDengue/PySUS/commit/12b21de78a6220ca8d158a39ccd6f152f5b343e0))

## [0.9.2](https://github.com/AlertaDengue/PySUS/compare/0.9.1...0.9.2) (2023-04-20)


### Bug Fixes

* **version:** linter online_data to bump version ([#128](https://github.com/AlertaDengue/PySUS/issues/128)) ([eddb93c](https://github.com/AlertaDengue/PySUS/commit/eddb93c5094a7bfebca51d57f33fbc3891f1d54f))

## [0.9.1](https://github.com/AlertaDengue/PySUS/compare/0.9.0...0.9.1) (2023-04-12)


### Bug Fixes

* **docs:** fix docs and add test for notebooks ([ebedd2b](https://github.com/AlertaDengue/PySUS/commit/ebedd2bec2d9be58f9eb2aa5433a11d04a688379))

## [0.9.0](https://github.com/AlertaDengue/PySUS/compare/0.8.0...0.9.0) (2023-03-28)


### Features

* **sinan:** add more parsed columns to final dataframe ([cc653f0](https://github.com/AlertaDengue/PySUS/commit/cc653f0dfc4793a50ed5b76d1ed40520ae4e75f9))


### Bug Fixes

* **notebooks:** update notebooks ([12dbb16](https://github.com/AlertaDengue/PySUS/commit/12dbb16e24208f7c24b0f790259d55bcad3b4562))
* **notebook:** update download references in notebooks ([3682fe6](https://github.com/AlertaDengue/PySUS/commit/3682fe61be27235c806ec90112157b0b366bb4af))


### Performance Improvements

* **sinan:** remove unnecessary cwd's in FTP_SINAN ([#123](https://github.com/AlertaDengue/PySUS/issues/123)) ([5199685](https://github.com/AlertaDengue/PySUS/commit/519968537c6493c19e65de0e9e1856c47850e4dd))

## [0.8.0](https://github.com/AlertaDengue/PySUS/compare/0.7.0...0.8.0) (2023-03-14)


### Features

* **SINAN:** moving EGH changes to PySUS ([72f2d93](https://github.com/AlertaDengue/PySUS/commit/72f2d938e4d4b742b730d5e599564357a05825f8))

## [0.7.0](https://github.com/AlertaDengue/PySUS/compare/v0.6.4...0.7.0) (2023-02-06)


### Features

* **semantic-release:** add semantic release to the project ([#114](https://github.com/AlertaDengue/PySUS/issues/114)) ([b079089](https://github.com/AlertaDengue/PySUS/commit/b0790898785666f588e716e02acdcc536d06c2e5))


### Bug Fixes

* **sinan:** remove hardcoded data path when extracting from pysus ([#113](https://github.com/AlertaDengue/PySUS/issues/113)) ([c56d2b1](https://github.com/AlertaDengue/PySUS/commit/c56d2b1a0ecf4e0d8efa4ea76e8cae3decc788d5))
* **sm-release:** fixing wrong branch on semantic-release ([#115](https://github.com/AlertaDengue/PySUS/issues/115)) ([c22f13c](https://github.com/AlertaDengue/PySUS/commit/c22f13c9c12fa3df45045db6b9d346318a2f25ef))
