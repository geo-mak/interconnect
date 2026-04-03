# Project's policies and management guidelines

## AI usage
Since I started tinkering with the so-called "AI", namely "LLMs", I was both impressed and skeptical, 
impressed for what has been achieved and skeptical of what it could be done with it compared to the "hype" surrounding this technology.

To make this section short and straightforward, I have used this technology in the context of software development exclusively for:

- Prototyping of designs where code quality or strict correctness doesn't matter compared to the effort of evaluating a strategy in general.

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

Using of LLMs for anything else remains a personal choice, as long the internal policy remains respected.

This project is not against automation, in fact it is **designed** to empower digital transformation and automation. 
There are social aspects to automation that need addressing in a proper way that maintains the social order and human dignity, but I think that we as humanity are capable of doing this if we talk to each other in an organized and transparent manner instead of being reactive and mean to each other.

The main concerns are the technology deployed for automation and the authenticity and trust regarding human communication.

ML is fundamentally a recognition technology suitable for domains that can't be easily translated to semantic rules. Using this technology for making a natural language interface wired to semantic components or for translation on-the-fly (think tourists) is one thing, and using it for high-precision creative tasks are totally different things.

Automation is fundamentally a **design-problem** not a **data-problem**. The trend of using LLMs as agents is a primitive and lazy approach, because it tries to glue a pile on top of an existing pile of outdated and bad designs. 

Moreover, people who are enthusiastic about the trend of of using LLMs as one-stop-shop agents fail to realize that the "false" sense of empowerment they might feel is caused by sheer complexity of the current systems and tools, which is in part a by-product of bad accumulated designs of the last decades, and an outcome to a trend of startups to pile up whatever in order to make some **noise/news**.

So the better approach could be trying to reduce the need to know and maintain a lot of arcane noisy things by means of:

- Better understanding of the current designs, and their history (It is not, it has been made as such, so why).

- Making programming languages as **integrated programming systems** with various components and tools to aid development.

- Better consistency and standardization.

- Proper and better **documentations** that are easily accessible and searchable.

- Redesigning for **effective** simplicity (e.g. Reducing noise and condensing the design).

The main point here is to **design/redesign** for the problem instead of throwing **data and statistical engines** on the problem, which would render a **trustworthy** and **efficient** solution to it.

I respect other's choices, but I think the current trend is fundamentally wrong, because it induces piling up complexity, without rethinking the foundations.