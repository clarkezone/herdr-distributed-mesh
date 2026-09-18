# Herdr Mesh: connect two computers and run a task

Use one command, **`herdr-mesh`**, on both computers. Each computer has one
background mesh connection. The dashboard, diagnostics, and operator commands
share that connection; they do not need separate Tailscale setup.

The examples name the computers **desktop** and **laptop**. Desktop coordinates
the mesh and can run agents. Laptop can run agents too. Either computer can
control the mesh.

## Before you start

On both computers, install the same build of `herdr-mesh.exe`, plus Herdr and Git
on PATH. Install and sign in to the provider you want to use; the default is
Copilot. A Herdr terminal window does not need to be open.

You need a Tailscale account and permission to configure its private network.
Tailscale calls that network a **tailnet**. Use its name instead of `example.com`
below. A separate system Tailscale installation is not required for the embedded
mesh connection.

This first-run experience targets Windows. The background process survives
closing your terminal and starts again when you sign in to Windows. It is not
a boot-before-sign-in Windows service.

## 1. Create the mesh on desktop

```powershell
herdr-mesh init --tailnet example.com --name desktop
```

Follow the prompts. First-time network configuration needs a **Tailscale API
access token**: a secret that lets setup configure the network on your behalf.
The command explains where to obtain it and accepts a hidden paste. It is not
an agent/provider credential, and you do not put it in command arguments or
environment-variable setup blocks. Setup does not retain it for normal use.

Review the proposed network changes before approving them. Existing unrelated
rules are preserved. If the network already contains broad access rules, setup
does not silently remove them.

Complete the displayed Tailscale browser sign-in when requested. If you run the
command remotely without a browser, open the displayed URL in your browser.
If your tailnet requires device approval, complete that approval too.

When ready, the command prints the exact join command for another computer.
It uses the coordinator's **actual full MagicDNS name**, including its tailnet
suffix. Do not construct that address yourself.

## 2. Join from laptop

Copy the command printed on desktop, choosing `laptop` as this computer's name.
For example, if desktop printed the following address:

```powershell
herdr-mesh join --server herdr-mesh-desktop.example.ts.net --name laptop
```

Sign in to the same tailnet in the browser when prompted. You do not need an API
access token on laptop or an enrollment key copied from desktop.

The coordinator address and default port (`50052`) are saved. You do not repeat
them on later commands.

## 3. Check the mesh from either computer

```powershell
herdr-mesh nodes
herdr-mesh doctor
```

Confirm both computers are connected and ready to manage Herdr sessions.
A running background process alone is not evidence that its worker is ready.

For the browser dashboard:

```powershell
herdr-mesh dashboard
```

For local connection status:

```powershell
herdr-mesh status
```

These commands reuse the existing connection. You can run another command while
watching output or using the dashboard.

## 4. Register an existing project

Prepare an existing Git checkout on each computer where you want to run work.
Register its path centrally, from either computer:

```powershell
herdr-mesh project add demo --node desktop --path C:\src\demo
herdr-mesh project add demo --node laptop --path C:\src\demo
```

Each path belongs to the selected computer, not necessarily the computer where
you typed the command. The paths can be different. Registration does not clone
repositories or copy project files.

## 5. Run and observe a task

```powershell
herdr-mesh agent start smoke --node laptop --project demo --prompt "Say hello"
herdr-mesh agent follow smoke --node laptop
```

Start ensures a named `main` Herdr session, opens or reuses the project's existing
checkout workspace, and launches a dedicated Copilot agent named `smoke`.
It does not create a Git worktree or attach a Herdr frontend. Use `--session`
or `--provider` when you want a different session or provider.

Canceling follow stops watching, not the agent. To stop that agent:

```powershell
herdr-mesh agent stop smoke --node laptop
```

The application retains the underlying session and terminal identities for safe
targeting. You do not copy those IDs or invent retry keys. If a name is ambiguous
or its target has been replaced, the command stops with an explanation rather
than guessing. If a launch outcome is uncertain, inspect it instead of starting
another agent under a new name.

An agent name stays bound to its original launch, including after stop. Repeating
the same start is a retry, not a new launch. Once the previous outcome is known,
choose a fresh name for a genuinely new run.

An accepted prompt is not proof the provider completed the task. Check the
actual response. Provider sign-in and permission prompts are not approved
automatically.

## Existing installations and advanced operations

Do not delete identity directories to make setup run again. Existing managed
configuration is checked rather than silently overwritten; older independently
running roles need an explicit migration decision.

For MCP, configure your MCP client to run:

```powershell
herdr-mesh mcp
```

For explicit role management, worktree creation, detailed receipts, and offline
maintenance, use the [advanced operator reference](advanced-operator-guide.md).
Its per-role backup commands do not back up the shared managed installation;
do not apply them to individual folders inside the managed state directory.
The [Windows acceptance matrix](windows-live-validation.md) is for deeper
release validation, not a prerequisite for trying the mesh.
