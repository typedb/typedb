---
title: Advanced inference rules
keywords: setup, getting started
last_updated: April 2018
summary: In this lesson you will learn more advanced uses of reasoning and how to chain inference rules.
tags: [getting-started, graql]
sidebar: academy_sidebar
permalink: ./academy/advanced-rules.html
folder: academy
toc: false
KB: academy
---

In the last session you have built a Reasoner rule to connect the articles in your knowledge graph to the relevant bonds according to the query you have been examining since the start of module 2. If everything went right, you should have a file called `rules.gql` that looks like this:

```graql
define

referendum-bond sub rule

  when {
        $article isa article has subject "Italian Referendum";
        $platform isa oil-platform has distance-from-coast <= 18;
        $italy isa country has name "Italy";
        (located: $platform, location: $country) isa located-in;
        (owned: $platform, owner: $company) isa owns;
        (issued: $bond, issuer: $company) isa issues;
      }

  then {
        (information: $article, affected: $bond) isa affects;
      };
```

This rule alone would work, but it is very specific. Besides, it would not link the articles to the oil platform but only to the bonds. And the latter connection is definitely an indirect one.

Let’s fix this.

## Breaking the rule
If you think about it again, there are two levels into answering the question "what are the bonds affected by the Italian referendum?". As I said above, that connection is indirect, but what we are really asking is: "What are the oil platforms affected by the Italian referendum?" and then "What are the bonds issued by companies owning those platforms?".

Let’s do it step by step. The first stage is to rewrite the rule above so that it links the articles to the oil platforms instead of the bonds. This should be pretty easy.

```graql
define

article-platform  sub rule

  when {
        $article isa article has subject "Italian Referendum";
        $platform isa oil-platform has distance-from-coast <= 18;
        $italy isa country has name "Italy";
        (located: $platform, location: $country) isa located-in;
    }

  then {
        (information: $article, affected: $platform) isa affects;
    };
```

The rule above will link the articles about the Italian referendum to the relevant oil platform instead of the bond. What should be done next? How do we translate the question "What are the bonds issued by companies owning those platforms?" into a rule?

Once again it is very easy:

```graql
define

article-bond  sub rule

  when {
        $article isa article has subject "Italian Referendum";
        $platform isa oil-platform;

        # The following relationship is not in the data
        # This rule needs the rule above to

        (information: $article, affected: $platform) isa affects;

        (owned: $platform, owner: $company) isa owns;
        (issued: $bond, issuer: $company) isa issues;
      }

  then {
        (information: $article, affected: $bond) isa affects;
    };
```

This new rule checks the articles about the Italian referendum linked to a platform and then links the same articles to bonds issued by companies owning those platforms.

But we already know that in our data there are no links between articles and anything else, let alone oil platforms, so how can this rule work? The answer is that the Reasoner knows that the first rule can link the articles to oil platforms and this means, that the results of the first rule can in fact change the results of the second one. It takes care of considering the rule in the correct order no matter how many you write and chain them for you. It’s a tiny bit of Grakn magic.


## But… why?
Ok, we have created two rules to link the articles about the Italian referendum to the relevant bonds. We already achieved that with only one rule so why would we want to write more?
There are in fact multiple reasons. The first one is that this way you will also have links between the articles and the appropriate oil platforms. This makes a lot of sense because the oil-platforms are directly affected by the Italian referendum whereas the bonds are not (at least not directly). 
Another reason can become evident if you modify the second rule a bit:

```graql
define

article-bond-new  sub rule

  when {
        (information: $article, affected: $platform) isa affects;
        (owned: $platform, owner: $company) isa owns;
        (issued: $bond, issuer: $company) isa issues;
      }

  then {
        (information: $article, affected: $bond) isa affects;
      };
```

This new rule is much more generic: it looks at every information that affects an oil platform and connects it to the bonds issued by the companies owning that platform. So it still works in our case, but it is more powerful. If we find an article about an oil spill happening at a specific platform and we add the link to the platform in our knowledge graph, the connection to the relevant bonds will also be visible.

This new rule is quite useful: it tells us that everything that affects an oil platform might potentially affect the bond issued by its owner.


## Can you explain that?
Save the two rules in their latest version in your `rules.gql` file and load them into your knowledge graph as you have learned in the [last module](./loading-files.html).

Open the dashboard and make sure that the inference is turned on as shown below

  ![Inference settings](/images/academy/5-reasoner/inference-settings.png)

Then run this query and see what happens:

```graql
match $x isa article;
$y isa bond; ($x, $y);
get;
```

Now compare the query above with its extended version:

```graql
match
$article isa article has subject "Italian Referendum";
$platform isa oil-platform has distance-from-coast <= 18;
(location: $country, located: $platform) isa located-in;
$country isa country has name "Italy";
(owner: $company, owned: $platform) isa owns;
(issuer: $company, issued: $bond) isa issues;
limit 3; get $bond, $article;
```

Not bad eh?

This is one good example that shows both how the Reasoner can be used for shortening queries and to infer new information (remember: the link between the articles and the bonds is not in the data!).

There is one more trick up the Reasoner’s sleeve to be learned before we proceed to the next topic.

Run the query to find the articles and the linked bonds again and click on the small circles connecting the bonds and the articles.

  ![Inferred relationships](/images/academy/5-reasoner/inferred-relationships.png)

If you look at the panel that shows up, you will see that that circle represents an "inferred relationship", i.e. a relationship that is not in your data, but has been added by the Reasoner.
So we know that those bonds and those platforms are connected, but why is that so? Double click the small circle and check what happens:

  ![Explanation example 1](/images/academy/5-reasoner/bond-explanation.png)

What you just witnessed is the action of the explanation facility. What this means is that when you double click on an inferred relationship, the Reasoner will tell you exactly why that relationship has been inferred and then you will see the explanation within the graph visualiser.

If you take a careful look at the explanation that has been added to your visualiser, you will see that there is another inferred relationship. This is result of the first inference rule.


### What have you learned?
In this lesson you have learned how rule chaining works in the reasoner (not much to do on your side: it just works) and how to use the explanation facility. There are still many things to discover, but you are ready to build powerful knowledge graphs with Grakn and take full advantage of its capabilities! Well done!

## What next
One last thing to do before we proceed to the next module: it is time to [review your knowledge](./reasoner-review.html) about logic reasoning.

