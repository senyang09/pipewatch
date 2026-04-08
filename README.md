# pipewatch

A lightweight CLI for monitoring and alerting on ETL pipeline health metrics in real time.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or log source:

```bash
pipewatch monitor --source postgresql://user:pass@localhost/mydb --interval 30
```

Set up an alert when a metric exceeds a threshold:

```bash
pipewatch alert --metric row_count --threshold 10000 --notify slack
```

Check the status of all tracked pipelines:

```bash
pipewatch status
```

Run `pipewatch --help` to see all available commands and options.

---

## Features

- Real-time metric polling from databases, files, or HTTP endpoints
- Configurable alert thresholds with Slack, email, and webhook support
- Simple YAML-based configuration for pipeline definitions
- Minimal dependencies, runs anywhere Python 3.8+ is available

---

## Configuration

Create a `pipewatch.yml` in your project root to define pipelines and alert rules. See the [docs](docs/configuration.md) for a full reference.

---

## Contributing

Pull requests are welcome. Please open an issue first to discuss any significant changes.

---

## License

This project is licensed under the [MIT License](LICENSE).