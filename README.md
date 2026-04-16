# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| packages/vertex-forager/src/vertex\_forager/clients/\_\_init\_\_.py |       35 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/base.py         |      170 |       28 |     84% |71, 250-252, 259-261, 273, 297-299, 306-308, 327-328, 333-335, 476-477, 553, 573-578, 591, 593-594 |
| packages/vertex-forager/src/vertex\_forager/clients/dispatcher.py   |       22 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/validation.py   |        5 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/\_\_init\_\_.py    |       12 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/checkpoint.py      |      341 |       29 |     91% |148, 155, 167, 170-171, 189, 247, 315, 462-463, 486, 508-509, 521, 660, 692, 708-709, 831-832, 847-848, 853-854, 863-864, 866-867, 929 |
| packages/vertex-forager/src/vertex\_forager/core/config.py          |      154 |       12 |     92% |64, 74, 107, 110, 225, 228, 231, 271, 274-275, 279, 281 |
| packages/vertex-forager/src/vertex\_forager/core/contracts.py       |       15 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/controller.py      |      229 |       30 |     87% |121, 244, 246, 250, 254, 258, 262, 264, 283-285, 303-306, 309-315, 326-328, 350-353, 362-363 |
| packages/vertex-forager/src/vertex\_forager/core/dlq.py             |      151 |       40 |     74% |47-48, 93-94, 120-122, 136-155, 179-189, 192-193, 240-251 |
| packages/vertex-forager/src/vertex\_forager/core/domain.py          |      107 |        8 |     93% |19, 45, 104-106, 164, 185, 189 |
| packages/vertex-forager/src/vertex\_forager/core/http.py            |      109 |       27 |     75% |16-17, 100, 104-112, 166-167, 189-191, 193-200, 203-204, 207 |
| packages/vertex-forager/src/vertex\_forager/core/lifecycle.py       |       70 |        4 |     94% |60-61, 64-65 |
| packages/vertex-forager/src/vertex\_forager/core/orchestration.py   |       74 |       10 |     86% |43-44, 92-93, 95-97, 125, 188-189 |
| packages/vertex-forager/src/vertex\_forager/core/pipeline.py        |      905 |      103 |     89% |113-114, 143-144, 151-152, 174, 176, 181-182, 189-192, 211-218, 339-341, 428-429, 439-440, 547-548, 570, 579-582, 775-776, 991-992, 996, 1010-1012, 1014-1017, 1084-1085, 1090-1096, 1138-1139, 1142-1143, 1146-1149, 1204, 1207-1212, 1278-1279, 1345-1365, 1460, 1473, 1531-1532, 1594-1595, 1606, 1609, 1671-1673, 1682-1683, 1742-1743, 1923-1924 |
| packages/vertex-forager/src/vertex\_forager/core/quality.py         |      115 |        5 |     96% |64, 88-89, 178-179 |
| packages/vertex-forager/src/vertex\_forager/core/retry.py           |       95 |        6 |     94% |74, 189-192, 203 |
| packages/vertex-forager/src/vertex\_forager/core/retry\_policy.py   |        9 |        2 |     78% |     14-15 |
| packages/vertex-forager/src/vertex\_forager/core/scheduler.py       |      155 |       17 |     89% |40, 42, 44, 136-142, 144, 146-147, 159, 175, 200-201 |
| packages/vertex-forager/src/vertex\_forager/core/sweep.py           |      143 |       68 |     52% |67, 82-94, 103-115, 127-139, 158-200, 210-239, 258-259, 265-266, 270-277, 295, 298, 301 |
| packages/vertex-forager/src/vertex\_forager/core/types.py           |       61 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/workerio.py        |       49 |        1 |     98% |       144 |
| packages/vertex-forager/src/vertex\_forager/core/writerflush.py     |      230 |       50 |     78% |109-122, 137-149, 171-174, 176, 214, 221, 257-294, 323, 340, 468-476, 479, 494, 512, 531 |
| packages/vertex-forager/src/vertex\_forager/routers/\_\_init\_\_.py |       19 |        1 |     95% |        55 |
| packages/vertex-forager/src/vertex\_forager/routers/base.py         |       23 |        1 |     96% |        65 |
| packages/vertex-forager/src/vertex\_forager/routers/errors.py       |       14 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/routers/jobs.py         |       25 |        3 |     88% |   124-127 |
| packages/vertex-forager/src/vertex\_forager/routers/transforms.py   |       43 |        5 |     88% |48, 66-67, 88-89 |
| packages/vertex-forager/src/vertex\_forager/writers/\_\_init\_\_.py |       44 |        6 |     86% |19, 32, 45-46, 83-87 |
| packages/vertex-forager/src/vertex\_forager/writers/base.py         |       26 |        5 |     81% |103-106, 147 |
| packages/vertex-forager/src/vertex\_forager/writers/constants.py    |       24 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/writers/duckdb.py       |      360 |       53 |     85% |141-142, 163, 239-240, 245-254, 260-266, 277, 376, 402, 408-409, 423-425, 430-431, 461-462, 536-537, 561-568, 578-582, 588, 619-629, 710-711 |
| packages/vertex-forager/src/vertex\_forager/writers/memory.py       |       64 |        8 |     88% |61, 116-122 |
| **TOTAL**                                                           | **3898** |  **522** | **87%** |           |


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