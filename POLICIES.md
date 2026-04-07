# Project's policies and management guidelines

## AI usage
Since I started tinkering with the so-called "AI", namely "LLMs", I was both impressed and skeptical, 
impressed for what has been achieved and skeptical of what it could be done with it compared to the "hype" surrounding this technology.

To make this section short and straightforward, I have used this technology in the context of software development exclusively for:

- Prototyping of designs where code quality or strict correctness doesn't matter compared to the effort of evaluating a strategy in general (AKA throwaway-code ).

- Prototyping of unit-tests according to a pre-exiting pattern, which I end up later refactoring and polishing manually.

- Codebase "research" for building a picture of a particular solution implemented in other codebases that forms a multi-file, multi-package puzzle in large codebases with the intent of narrowing the search-scope with various accuracy instead of getting lost in details of that codebase for days.

- Review of code I wrote when I was tired, and where any review could be better than nothing, even if the suggestions would be inaccurate or flat out nonsense.

I presume that this technology is here to stay, but I don't think it is important in the making of any serious software that people could rely on for these **main** reasons:

- It is inherently faulty in a domain where a trivial mistake could mean disasters or deaths.
- Writing code is not the challenge (at least in my case), most of my time is spent in design and finding optimization's tricks.
- Maintaining a codebase requires understanding its inner working in its entirety.
- Generating a pile to get it "polished" later is a naïve and misguided approach. Refactoring a foreign codebase is more time-consuming than writing from scratch.
- Distractive to crisp thinking, where a vivid attention must be given.
- Induces overconfidence and false sense of achievement.

So the reality is not black or white, it is actually very colorful, but the overhype and the unreasonable "bullying" in the last years indicate that the software industry in general is still in its infancy and not up to the task of understanding the effect of the increasing role of software systems in managing the modern world, from digital services to critical infrastructure, and taking it seriously.

Feedbacks, suggestions and ideas are welcomed, but this project does not have an open code-contribution model.
Contributions in a form of code and documents will be authored by the project's members and delegated persons **only**, 
hence there is no public policy of disclosure.

The project's internal policy in this regard specifies the following:

- Code that is intended to get merged into the codebase **must** be **fully** written and audited by humans, in addition to all related **comments** and **documentations**. 

- All documentations and publications made available by this project **must** be **fully** written and audited by humans.

Using of LLMs for anything else is a personal choice, as long the internal policy remains respected.

This project is not against automation, in fact it is **designed** to empower digital transformation and automation. 
There are social aspects to automation that need addressing in a proper way that maintains the social order and human dignity, but I think that we as humanity are capable of doing this if we talk to each other in an organized and transparent manner instead of being reactive and mean to each other.

The main concerns are the technology deployed for automation and the authenticity and trust regarding human communication.

ML is fundamentally a recognition technology suitable for domains that can't be easily translated to semantic rules. Using this technology for making a natural language interface wired to semantic components or for translation on-the-fly (think tourists) is one thing, and using it for high-precision creative tasks is a totally different thing.

Automation is fundamentally a **design-problem** not a **data-problem**. The trend of using LLMs as agents is a primitive and lazy approach, because it tries to glue a pile on top of an existing pile of outdated and bad designs. 

Moreover, people who are enthusiastic about the trend of using LLMs as one-stop-shop agents fail to realize that the "false" sense of empowerment they might feel is caused by sheer complexity of the current systems and tools, which is in part a by-product of accumulated outdated and bad designs of the last decades, and an outcome to a trend of startups to pile up whatever in order to make some **noise/news**.

As a little piece of ancient history related to this, I have proposed a project for integrating LLMs as automating agents at a university I was affiliated with back in **2023**, less than one year after releasing ChatGPT, where I was treated like a crackpot proposing a cracked idea, and my project ended up being rejected. This was part of my long-time quest to make organizations software-first (software defined) with a very high level of automation, where the human role lies in process design (domain-knowledge experts), policies, engineering, maintenance and support. 

After several months of proposing my project, Microsoft has released in early **2024** the copilot-architecture, which was in many ways resembling the core ideas in my project, despite the fact that my project was limited to operational risk management in terms of functionalities.

During that time I was continuously experimenting with the idea and writing prototypes in my free time using Python, until I reached a point where I have decided that this language is a dead end and switching now is better than reimplementing later, actually the performance I was experiencing with Python has pissed me off to a point where I dared to piss off Guido, the core team and the entire community of Python by proposing an AOT implementation with static typing (original [discussion](https://discuss.python.org/t/aot-instead-of-jit/51849)), and this was where I decided to roll up my sleeves, or more accurately, to take off my clothes and swim into the deep water of Rust and its libraries in a very serious and dedicated way. Other language like C/C++ were too old and messy to my taste, other newer languages were too unstable and less satisfying in terms of taste.

In that spirit, I have started several projects using Rust exclusively for exploring the data, connectivity and concurrency landscape of Rust and its sharp edges and limits, and to dig into the "unsafe" world of Rust and its internals in order make mistakes and learn things as early as possible. Many of these projects ended up being abandoned, as they have reached their limits of delivering viable lessons, other have been abandoned because other projects have entered the scene, e.g. SQL-API project (SQL-compiler, IR to dialect-specific SQL-code) has been abandoned when toasty has been started in late 2024..etc. SQL-API was the project which has "spawned" the `omnimap` project.

The point of mentioning this little piece of ancient history is that probably most of the current enthusiasts and preachers are **"too little, too late"** to the party, where they might think they are the "frontier thinkers" lecturing others how to adapt to the "new world order".

So the better approach could be trying to reduce the need to know and maintain a lot of arcane noisy things by means of:

- Better understanding of the current designs, and their history (It is not, it has been made as such, so why).

- Making programming languages as **integrated programming systems** with rich set of components and tools to aid development.

- Better consistency and standardization.

- Proper and better **documentations** that are easily accessible and searchable.

- Redesigning for **effective** simplicity (e.g. Reducing noise and condensing the design).

The main point here is to **design/redesign** for the problem instead of throwing **data and statistical engines** on the problem, which would render a **trustworthy** and **efficient** solution to it.

I respect others' choices, but I think the current trend is fundamentally wrong, because it induces piling up complexity, without rethinking the foundations.