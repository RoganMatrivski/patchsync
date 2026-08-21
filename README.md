# Patchsync

Sync directory (or a file) through Iroh network, in an rsync-like manner.

> [!WARNING]
> This library API may change!
> 
> You can use this library if you want. Expect breaking change.

## What's this

As description.

## Why make this

TL;DR, i need a tool at work, so i made this.

<details>
<summary>Long Post Below</summary>

This library original idea was spun from another project that i use at work `exec-sendrecv`. I was in need of an app which i can send loose build files, send them through Iroh, and execute them.

Why Iroh? Office network segmented as hell, and although i can use Tailscale, setting up commands to sync something with a tool + making it automatically run after sync is a hassle. So i quickly spin up some app with a help of (insert your favorite AI here).

It works fine at the start, but after too much feature to add and the networking side code looking too much of my homemade spaghetti, i spun it off.

</details>

## AI Policy

I'm not adverse against AI, i'm adverse against vibe-coding. As such, i try to not use AI willy-nilly. In a case when i do, i try to mark it in the commit message. "AI Generated" if it's >80% by AI, "AI Assist" otherwise. If it's only a snippet, i try to mark it as a comment.
