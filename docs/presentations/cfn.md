---
theme: gaia
_class: lead
paginate: true
backgroundColor: #fff
backgroundImage: url('https://marp.app/assets/hero-background.svg')
---

# CKB Fiber Network

the best thing you may have never heard of about bitcoin lightning network

---

## Mass adoption

Everyone is anticipating it, but can we live long enough to witness it?

---

### 2024, the year of BTC L2? All quiet on the LN front

![bg 90%](./ln-statistics.png)

---

### User experience of lightning network (or crypto currency in general)

![bg left:60% 80%](./terrible-ux.png)

---


## Rethinking lightning

Source: https://stacker.news/items/379225

![bg left:65% 90%](./rethinking-lightning-screenshot.png)

---

> Nowadays, the average lightning user actually isn't using lightning.

> First and foremost one of the hardest UX challenges of lightning is channel liquidity.

> The other major pain point of lightning is the offline receive problem.

> Combining existing large scale lightning infrastructure with self-custodial solutions sadly, isn't totally possible.

> So how do we scale ownership? Simply put, the answer today is custody.

---

> Are we doomed then? Is there no way to scale bitcoin in a self-sovereign way? Luckily, the answer is no, but we need some soft-forks. Covenants are the way to scale bitcoin ownership.

![bg left:50% 80%](./covenants.jpg)

---

### What covenants can do?

Source: https://covenants.info/overview/summary/

![./covenant-proposals.jpg](./covenant-proposals.jpg)


---

### When can we use covenants on BTC?

![bg left:60% 80%](./covenants-when.png)

---

## Join BTC by CKB

![bg 45%](./spongebob-squarepants-strong.gif)

---

## Wait, does CKB have covenants already?

![./always-been-there.png](./always-been-there.png)

---

They have always been there. Just too trivial to give a dedicated term.

![bg left:70% 80%](./ckb-vm-syscalls.png)

---

### And can CKB do that?

![bg 60%](./ckb-with-footnote.png)

---

### Request for fact-checking

- You are welcome fact-check my hasty conclusion above (it's backed by only over-confidence). 

- I will not fix any inaccuracy in my slides, as CKB is easily fixable.

---

## Introducing CKB Fiber Network (CFN)

![](./cfn=ckb+ln.jpg)

---

```
    BTC      L1
   /   \
  /     \
LN      CKB  L2
  \     /
   \   /
    CFN    L2+L2=L3 or L4?
```

- LN: Instant, Infinitely Scalable P2P Payment System
- CKB: Unmatched Flexibility and Interoperability

Call this $L_\infty$ instead of $L_3$ or $L_4$.

---

### High level overview of CFN

![bg 90%](./cfn-ln.png)

---

- Same building blocks as lightning network (HTLC and revocation)
- Native multiple assets support (extremely versatile thanks to xUDT's extensibility)
- Cross-chain payment channel network (available now, made only possible by CKB-VM's flexibility)

---

### Demo time and some bad news

We only have time to show some staged animations.

TODO: show some testnet transaction screenshots on the explorer websites.

---

## Conclusion

---

### CFN as of today

- Same security assumption as bitcoin lightning network
- Native multi-assets payment channel network
- Native bitcoin lightning network interoperability with atomic 2-way transfers

---

### CFN as of tomorrow

- Achieve Feature parity with bitcoin lightning network (watch tower, multiple-hop network)
- Leverage existing BTC lightning network infrastructure for payment routing
- Rethink payment channel network with CKB's extensibility and programmbility
  - Highly-articipated lightning network features made possible by covenants (e.g. Non Interactive Channels)
  - And beyond

---

## Join the force

![bg left:60% 80%](./friends-are-stronger-together.webp)

Come and build.
Life is too short,
for all the nicest BIPs to land.


- https://github.com/nervosnetwork/cfn-node
- https://github.com/nervosnetwork/cfn-scripts