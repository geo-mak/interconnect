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