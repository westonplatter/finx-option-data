<a name="unreleased"></a>
## [Unreleased]


<a name="0.0.5"></a>
## [0.0.5] - 2022-01-08
### Bug Fixes
- **configs:** bump post process memory size to 10G (Weston Platter) [goto](https://github.com/westonplatter/finx-option-data/commit/b77b989f7f3a3ee99cb67d1b63996226975f8289)

### Code Refactoring
- **logging:** move logging setup to helpers file (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/b4ca14ed3d9a7dee08b2667dc0458b2d165d5476)

### Features
- download parquet files locally (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/2f2001c55bb0512530fa7c6e95e92dd6451a9543)
- recompose data into 1 min bars ([#23](https://github.com/westonplatter/finx-option-data/issues/23)) (GitHub) [goto](https://github.com/westonplatter/finx-option-data/commit/24b4ddc86fa58366870c6b088b5b0b1eb121b41c)
- **cli:** add Click command to post process options data. (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/d835c6801f75dc1d484bd7112f7b092c645e2627)
- **core:** added tooling and setup for CHANGELOG.md (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/5264aebf5bb8e95a6ebf4bfe7d8b76db4c967aa0)
- **core:** add function to calc run time length. apply it in post processing. (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/ac537c3498e3ad47e6f2626da85cfc04b80626b3)

### Performance Improvements
- **data:** adjust the Pandas group by logic in post processing. 60s => 2s (westonplatter) [goto](https://github.com/westonplatter/finx-option-data/commit/fbcdaf0430d14ea4ef32f9fc4f07bcc00f85e111)


<a name="0.0.3"></a>
## [0.0.3] - 2022-01-08

<a name="0.0.4"></a>
## [0.0.4] - 2021-10-31

<a name="0.0.2"></a>
## [0.0.2] - 2021-10-27
### Features
- add setup instructions and handle password issue ([#19](https://github.com/westonplatter/finx-option-data/issues/19)) (GitHub) [goto](https://github.com/westonplatter/finx-option-data/commit/52270ce6b430cd920f32f0c370d4fe6841f3b5a4)


<a name="0.0.1"></a>
## 0.0.1 - 2021-06-12

[Unreleased]: https://github.com/westonplatter/finx-option-data/compare/0.0.5...HEAD
[0.0.5]: https://github.com/westonplatter/finx-option-data/compare/0.0.3...0.0.5
[0.0.3]: https://github.com/westonplatter/finx-option-data/compare/0.0.4...0.0.3
[0.0.4]: https://github.com/westonplatter/finx-option-data/compare/0.0.2...0.0.4
[0.0.2]: https://github.com/westonplatter/finx-option-data/compare/0.0.1...0.0.2
