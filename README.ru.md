# mac-worker

[English](README.md) · **Русский** · [Сайт документации](https://kirchik95.github.io/mac-worker/ru/)

<!-- Keep README.md and README.ru.md in sync. Detailed instructions belong in docs/getting-started.md. -->

**Отправьте задачу на другой Mac. Получите Git-ветку с результатом.**

Запускайте Codex, Cursor, OpenCode или Claude Code на свободных Mac через SSH. Каждая задача получает отдельный Git worktree; результат вы проверяете и вливаете на ноутбуке.

## Что нужно

- Два Mac с Apple Silicon и Git: ноутбук и хотя бы один рабочий Mac.
- Доступ к рабочему Mac по SSH-ключу и включённый Remote Login. Во время выполнения задач Mac должен оставаться включённым и не уходить в сон.
- Один установленный агент на рабочем Mac с выполненным входом в аккаунт. В примерах используется Codex.

[Подготовка рабочего Mac](docs/setup-macos-worker.md) описывает SSH, агентов и инструменты проекта. Запускайте доверенные задачи: агент имеет доступ к файлам и учётным данным аккаунта на рабочем Mac.

## Быстрый старт

Все команды ниже выполняются на **ноутбуке**.

### 1. Установите CLI на ноутбук

Соберите из исходников с помощью Rust и Git:

```bash
git clone https://github.com/kirchik95/mac-worker.git
cd mac-worker
cargo build --locked --release
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/worker "$HOME/.local/bin/worker"
export PATH="$HOME/.local/bin:$PATH"
```

Добавьте строку `export PATH` в `~/.zprofile`, чтобы команда была доступна в новых терминалах. Панель управления входит в сборку. [Варианты установки и релизы](docs/getting-started.md#1-install-the-cli-on-your-laptop).

### 2. Подключите рабочий Mac

```bash
worker init yourname@mini.local --agent codex
```

Замените SSH-адрес на свой. `init` установит компонент mac-worker на рабочий Mac и проверит вход агента в аккаунт. Если команда сообщит о недостающем шаге настройки, выполните его и повторите запуск. Для другого агента укажите его через `--agent` здесь и при отправке задачи.

### 3. Получите первую ветку

Откройте свой проект — Git-репозиторий, в котором есть хотя бы один коммит:

```bash
cd /path/to/your/project
worker task submit --agent codex --wait \
  --prompt "Create SETUP_CHECK.md containing: mac-worker works."
```

Подставьте полученный идентификатор задачи вместо `<task-id>`:

```bash
worker task result <task-id>
worker task diff <task-id> --stat
worker task fetch <task-id>
```

`fetch` выведет Git-ссылку на результат для проверки. Текущая рабочая копия останется прежней; вы сами решаете, что вливать. По умолчанию задача берёт исходники из `HEAD`; для незакоммиченных изменений нужен [`--wip`](docs/getting-started.md#manual-cli).

## Повседневная работа

Откройте панель управления, чтобы следить за рабочими Mac и задачами:

```bash
worker dashboard
```

Чтобы поручать работу агенту на ноутбуке, [установите два навыка для работы с пулом](docs/getting-started.md#ask-your-laptop-agent) и попросите:

> Отправь задачу в пул: создай SETUP_CHECK.md со строкой mac-worker works. Дождись результата и покажи ветку.

По умолчанию очередью управляет ноутбук. Включите [удалённый контроллер](docs/usage.md#remote-controller), чтобы очередь и процессы управления задачами работали на постоянно включённом Mac.

## Документация

Подробные руководства ниже — на английском.

- [Полное руководство](docs/getting-started.md) — установка, навыки агента на ноутбуке, рабочий процесс и архитектура.
- [Настройка рабочего Mac](docs/setup-macos-worker.md) — SSH, вход в аккаунты агентов и зависимости проекта.
- [Справочник](docs/usage.md) — продолжение задач, параллельная работа, зависимости между задачами, настройки и известные ограничения.
- [Обновление и удаление](docs/getting-started.md#update-or-remove) — резервные копии, обновление компонентов на рабочих Mac и перезапуск панели управления.
- [Разработка и релизы](docs/releasing.md) · [Разработка панели управления](ui/README.md).

[Лицензия MIT](LICENSE)
