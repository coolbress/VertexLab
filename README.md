# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| packages/vertex-forager/src/vertex\_forager/clients/\_\_init\_\_.py |       43 |        1 |     98% |       197 |
| packages/vertex-forager/src/vertex\_forager/clients/base.py         |      166 |       22 |     87% |72, 76, 318-320, 327-329, 348-349, 354-356, 466-467, 545, 562-567, 580, 582-583 |
| packages/vertex-forager/src/vertex\_forager/clients/dispatcher.py   |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/validation.py   |        5 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/\_\_init\_\_.py    |       11 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/checkpoint.py      |      222 |       68 |     69% |124, 136, 139-140, 154, 261-262, 279, 294-295, 401-404, 420-423, 432-433, 474, 496-515, 528-529, 541-564, 581-584, 593-605 |
| packages/vertex-forager/src/vertex\_forager/core/config.py          |      252 |       20 |     92% |76, 86, 121, 124, 232, 235, 238, 241, 357, 369, 372-373, 377, 379, 384-385, 391-392, 448, 467 |
| packages/vertex-forager/src/vertex\_forager/core/contracts.py       |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/controller.py      |      219 |       38 |     83% |121, 247, 249, 257, 261, 265, 267, 283-285, 303-306, 309-315, 355, 361-363, 387-390, 395-404 |
| packages/vertex-forager/src/vertex\_forager/core/dlq.py             |      151 |       40 |     74% |47-48, 93-94, 119-121, 135-154, 178-188, 191-192, 239-250 |
| packages/vertex-forager/src/vertex\_forager/core/errors.py          |       38 |       12 |     68% |18-19, 24-25, 105-111, 117, 123, 129 |
| packages/vertex-forager/src/vertex\_forager/core/http.py            |      107 |       26 |     76% |16-17, 105, 109-117, 168-169, 187-189, 191-198, 201-202 |
| packages/vertex-forager/src/vertex\_forager/core/library.py         |       25 |        1 |     96% |        26 |
| packages/vertex-forager/src/vertex\_forager/core/lifecycle.py       |       74 |        6 |     92% |33-38, 70-71, 74-75 |
| packages/vertex-forager/src/vertex\_forager/core/orchestration.py   |       74 |       10 |     86% |43-44, 92-93, 95-97, 125, 188-189 |
| packages/vertex-forager/src/vertex\_forager/core/pipeline.py        |      896 |      121 |     86% |113-114, 140-141, 148-149, 171, 173, 178-179, 186-189, 208-215, 339-341, 420-421, 431-432, 484, 493-510, 561-562, 739-740, 961-962, 966, 980-982, 984-987, 1026-1027, 1053-1054, 1059-1065, 1107-1108, 1111-1112, 1115-1118, 1173, 1176-1181, 1247-1248, 1314-1328, 1423, 1435-1436, 1494-1495, 1557-1558, 1569, 1572, 1634-1636, 1645-1646, 1705-1706, 1715-1716, 1881-1882, 2053 |
| packages/vertex-forager/src/vertex\_forager/core/quality.py         |       90 |        4 |     96% |120-121, 228-229 |
| packages/vertex-forager/src/vertex\_forager/core/recover.py         |      114 |       18 |     84% |42-45, 89, 94, 144-146, 156-158, 178, 199, 221-224 |
| packages/vertex-forager/src/vertex\_forager/core/registries.py      |       44 |        5 |     89% |85, 90, 113, 117, 121 |
| packages/vertex-forager/src/vertex\_forager/core/retry.py           |       94 |        6 |     94% |73, 188-191, 202 |
| packages/vertex-forager/src/vertex\_forager/core/scheduler.py       |      155 |       17 |     89% |40, 42, 44, 136-142, 144, 146-147, 159, 175, 200-201 |
| packages/vertex-forager/src/vertex\_forager/core/sweep.py           |      143 |      104 |     27% |20, 63, 67, 82-94, 103-115, 127-139, 158-200, 210-239, 251-278, 290-318, 329-347 |
| packages/vertex-forager/src/vertex\_forager/core/types.py           |       61 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/workerio.py        |       51 |        1 |     98% |       140 |
| packages/vertex-forager/src/vertex\_forager/core/writerflush.py     |      260 |       75 |     71% |108-121, 136-148, 170-173, 175, 213, 220, 256-293, 322, 340, 468-476, 479, 494, 513-530, 560, 577, 586-627, 632 |
| packages/vertex-forager/src/vertex\_forager/routers/\_\_init\_\_.py |       19 |        1 |     95% |        79 |
| packages/vertex-forager/src/vertex\_forager/routers/base.py         |       23 |        1 |     96% |        65 |
| packages/vertex-forager/src/vertex\_forager/routers/errors.py       |       14 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/routers/jobs.py         |       25 |        3 |     88% |   124-127 |
| packages/vertex-forager/src/vertex\_forager/routers/transforms.py   |       43 |        5 |     88% |48, 66-67, 88-89 |
| packages/vertex-forager/src/vertex\_forager/writers/\_\_init\_\_.py |       26 |        3 |     88% |     58-63 |
| packages/vertex-forager/src/vertex\_forager/writers/base.py         |       26 |        5 |     81% |103-106, 147 |
| packages/vertex-forager/src/vertex\_forager/writers/constants.py    |       24 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/writers/duckdb.py       |      308 |       41 |     87% |141-142, 163, 242-243, 251-257, 313, 325-326, 348-350, 355-356, 386-387, 461-462, 486-493, 503-507, 513, 544-554, 636-637 |
| packages/vertex-forager/src/vertex\_forager/writers/memory.py       |       60 |        8 |     87% |56, 111-118 |
| **TOTAL**                                                           | **3905** |  **662** | **83%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/coolbress/VertexLab/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/coolbress/VertexLab/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fcoolbress%2FVertexLab%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.