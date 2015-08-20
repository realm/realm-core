# A primer on Realm {#primer_intro} #

First of all, congratulations, and welcome! If you're reading this, it most
probably means that you just joined the Realm Core team. These first few days
have probably been exhausting; consider this an attempt to make your life a bit
easier.

## Tools and links ##

Here are a few bookmarks of the things that might be useful to you:

- [Slack (instant messaging)][slack];
- [Waffle (issue tracking)][waffle];
- [Wiki (company-wide)][wiki].

PS: If you're a Linux user, you might be interested by [ScudCloud][scudcloud],
a desktop application for Slack.

### Can I...? ###

Feel free to ask questions. Heck, you're *encouraged* to do so. If you can take
notes along the way, add non-obvious stuff to documentation, all the better.
You're free to do anything you want; whichever method gets you online and
contributing quickest is valid.

If you get stuck, or don't understand something, you can ask a question on
`#core`. If nobody answers within a few minutes, ask someone in the office if
they can help you out. People will tell you if they're busy and don't want to
lose their train of thought. If you're not in the office, pester people with
instant messages and whatnot. They have the knowledge, and they should've
documented it or made sure you had access to the documentation.

## Getting the code ##

You should be familiar with `git`, but just in case, here's [a
cheat-sheet][git-cheat-sheet]. Assuming you have `git` installed on your
computer, you should be ready to grab the latest version:

    $ git clone git@github.com:realm/realm-core.git

There's a number of tools that are required in order to build the code, but
those are documented on the main page (or in `README.md`), so we won't go into
the details here. Please have a read through `README.md`; it's a well-written
document that provides all the information required to understand the build
system on most platforms.

## Getting started ##

At this point, you should have the code built and installed, and probably even
ran the unit test suite. If not, please go back to `README.md` :).

### CLI (Linux/OSX) ###

This section assumes that you're running a UNIX-like environment (Linux, OSX,
etc) and are fairly familiar with the command line tools. One of the more
effective ways of hacking on Core is to use a fairly traditional red-green
cycle: write a test, ensure it fails, then fix the code until the test passes.

The unit test framework is *quick* (no, really), but it still takes a few
seconds to run, so using the built-in filtering abilities is definitely a must.

    # $EDITOR test/test_... # write test, ensure it fails
    $ UNITTEST_FILTER="MyTest_Foo" sh build.sh check-debug # fails, all good
    $ $EDITOR src/realm/... # modify the code
    $ UNITTEST_FILTER="MyTest_Foo" sh build.sh check-debug # check if it passes
    # All good!

It's also a good idea to check that your tests pass in "release" mode. For
this, simply replace `check-debug` with `check`.

### GUI (XCode/VisualStudio) ###

If you're using VS or XCode, please ask someone from the team to complete this
document and help you get started.

## What to start with? ##

This is a bit more difficult to answer, and really depends on how you learn.
Some people simply read through the code, and get a hold that way. Others like
to read through the `git` history to see what the moving parts are, and focus
(or not) on those. It's up to you.

How can you start contributing? Well, head over to [Waffle][waffle], and look
at the currently open issues. Get someone to explain how Waffle is organised.
Very roughly: the left hand side is more urgent, the right side is less urgent.
Read through some P2 or P3 issues, preferably, look through issues that aren't
assigned to anyone (to avatar stuck to it), and if there's anything you feel
capable of tackling; just go ahead and assign the issue to yourself and get
going!

If you have trouble understanding what the issues are about, ask the person who
created it, or maybe just give a shout in `#core` to see if anyone is familiar
with it. If you get interesting information, add it to the issue, so that the
information isn't lost.

Hopefully, by the time you've finished reading through this primer, most issues
and tasks will make sense. This covers the very basics, and the intro to this
primer. Let's head on to the next part: \ref primer_architecture
"Architecture".

[slack]: https://realmio.slack.com
[waffle]: https://waffle.io/realm/realm-core
[wiki]: https://github.com/realm/realm-wiki/wiki
[scudcloud]: https://github.com/raelgc/scudcloud
[git-cheat-sheet]: http://www.git-tower.com/blog/git-cheat-sheet/
