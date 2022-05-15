const axios = require("axios");
const { db } = require("./database");
const { query, where, getDocs, collection, collectionGroup } = require("firebase/firestore");
const moment = require("moment-timezone");
const { pick, map, pipe, values, head, identity, of, curry, sum, flatten, not, uniq, toLower, reject, mergeAll, paths } = require("ramda");
const { size, isUndefined, isEmpty, toNumber, orderBy: lodashorderby, compact, isNil, flattenDeep } = require("lodash");
const { from, zip, of: rxof, catchError, throwError, iif } = require("rxjs");
const { concatMap, map: rxmap, filter: rxfilter, reduce: rxreduce, defaultIfEmpty } = require("rxjs/operators");
const { get, all, mod, matching } = require("shades");
const { logroupby, lokeyby, louniqby, lofilter, pipeLog, loget, loomitby, isEmail, isIPv4, isIPv6 } = require("helpers");
const { Facebook: RoasFacebook } = require("roasfacebook");
const fetch = require("node-fetch");

const loorderby = curry((path, direction, data) => lodashorderby(data, path, direction));

const Timestamp = {
    toUTCDigit: (timestamp) => {
        let regex_expression = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;

        let date = moment(timestamp, "X").format("YYYY-MM-DD");
        let date_is_valid = regex_expression.test(date);

        if (!date_is_valid) {
            return timestamp / 1000;
        } else {
            return timestamp;
        }
    },
};

const Facebook = {
    ads: {
        details: {
            get: ({ ad_ids = [], user_id, fb_ad_account_id, date } = {}) => {
                let func_name = `Facebook:ads:details`;
                console.log(func_name);

                if (!ad_ids) return throwError(`error:${func_name}:no ad_ids`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(ad_ids).pipe(
                    concatMap((ad_meta_data) => {
                        let { ad_id } = ad_meta_data;
                        let ad_args = { ad_id, date, user_id, fb_ad_account_id };

                        return Facebook.ad.db.get(ad_args).pipe(
                            concatMap((ad) => iif(() => !isEmpty(ad), rxof(ad), Facebook.ad.api.get(ad_args))),
                            rxfilter(pipe(isEmpty, not))
                        );
                    }),
                    defaultIfEmpty([])
                );
            },
        },
    },

    ad: {
        details: (ad) => {
            let func_name = `Facebook:ad:details`;
            console.log(func_name);

            if (ad.details) {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.details.ad_id,
                    asset_name: ad.details.ad_name,
                    campaign_id: ad.details.campaign_id,
                    campaign_name: ad.details.campaign_name,
                    adset_id: ad.details.adset_id,
                    adset_name: ad.details.adset_name,
                    ad_id: ad.details.ad_id,
                    ad_name: ad.details.ad_name,
                    name: ad.details.ad_name,
                };
            } else {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.id,
                    asset_name: ad.name,
                    campaign_id: ad.campaign_id,
                    campaign_name: ad.campaign_name,
                    adset_id: ad.adset_id,
                    adset_name: ad.adset_name,
                    ad_id: ad.id,
                    ad_name: ad.name,
                    name: ad.name,
                };
            }
        },

        db: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:db:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(RoasFacebook({ user_id }).ad.get_from_db({ ad_id })).pipe(
                    concatMap(identity),
                    rxfilter((ad) => !isEmpty(ad)),
                    rxmap(Facebook.ad.details),
                    defaultIfEmpty({}),
                    catchError((error) => {
                        console.log("Facebook:ad:db:get:error");
                        console.log(error);
                        return rxof({ ad_id, error: true });
                    })
                );
            },
        },

        api: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:api:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let facebook = RoasFacebook({ user_id });

                return from(facebook.ad.get({ ad_id, date, fb_ad_account_id })).pipe(
                    rxmap(pipe(values, head)),
                    rxfilter((ad) => !isUndefined(ad.id)),
                    concatMap((ad) => {
                        let adset = Facebook.ad.adset.api.get({ adset_id: ad.adset_id, user_id, date, fb_ad_account_id });
                        let campaign = Facebook.ad.campaign.api.get({ campaign_id: ad.campaign_id, user_id, date, fb_ad_account_id });

                        return zip([adset, campaign]).pipe(
                            rxmap(([{ name: adset_name }, { name: campaign_name }]) => ({ ...ad, adset_name, campaign_name })),
                            rxmap(Facebook.ad.details)
                        );
                    }),
                    defaultIfEmpty({}),
                    catchError((error) => {
                        console.log("Facebook:ad:api:get:error");
                        console.log(error);
                        return rxof({ ad_id, error: true });
                    })
                );
            },
        },

        adset: {
            api: {
                get: ({ adset_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:adset:api:get`;
                    console.log(func_name);

                    if (!adset_id) return throwError(`error:${func_name}:no adset_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.adset.get({ adset_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)), defaultIfEmpty({}));
                },
            },
        },

        campaign: {
            api: {
                get: ({ campaign_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:campaign:api:get`;
                    console.log(func_name);

                    if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.campaign.get({ campaign_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)));
                },
            },
        },
    },
};

const Event = {
    ad: {
        id: ({ fb_ad_id, h_ad_id, ad_id } = {}) => {
            let func_name = `Event:ad:id`;
            console.log(func_name);

            if (ad_id) {
                return ad_id;
            }

            if (fb_ad_id && h_ad_id) {
                if (fb_ad_id == h_ad_id) {
                    return fb_ad_id;
                }

                if (fb_ad_id !== h_ad_id) {
                    return h_ad_id;
                }
            }

            if (fb_ad_id && !h_ad_id) {
                return fb_ad_id;
            }

            if (h_ad_id && !fb_ad_id) {
                return h_ad_id;
            }
        },
    },

    ip: {
        v4: (event) => {
            let func_name = `Event:ip:v4`;
            console.log(func_name);
            return pipe(get("ipv4"))(event);
        },

        v6: (event) => {
            let func_name = `Event:ip:v6`;
            console.log(func_name);
            return pipe(get("ipv6"))(event);
        },
    },

    user_agent: (event) => {
        let func_name = `Event:user_agent`;
        console.log(func_name);
        return pipe(get("userAgent"))(event);
    },

    get_utc_timestamp: (value) => {
        console.log("get_utc_timestamp");

        let timestamp;

        if (get("created_at_unix_timestamp")(value)) {
            timestamp = get("created_at_unix_timestamp")(value);
            console.log(timestamp);
            return timestamp;
        }

        if (get("utc_unix_time")(value)) {
            let timestamp = get("utc_unix_time")(value);
            console.log(timestamp);
            return timestamp;
        }

        if (get("utc_iso_datetime")(value)) {
            let timestamp = pipe(get("utc_unix_time"), (value) => moment(value).unix())(value);
            console.log(timestamp);
            return timestamp;
        }

        timestamp = get("unix_datetime")(value);
        console.log(timestamp);

        if (!timestamp) {
            console.log("notimestamp");
            console.log(value);
        }

        return timestamp;
    },
};

const Events = {
    user: {
        get: {
            ipv4: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv4";
                console.log(func_name);
                let events_query = query(collection(db, "user_events"), where("roas_user_id", "==", roas_user_id), where("ipv4", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },

            ipv6: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv6";
                console.log(func_name);
                let events_query = query(collection(db, "user_events"), where("roas_user_id", "==", roas_user_id), where("ipv6", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },
        },
    },
};

const ClickFunnels = {
    // queries: {
    //     email_query: (email) =>
    //         from(getDocs(query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("email", "==", email)))).pipe(
    //             rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
    //             defaultIfEmpty([])
    //         ),

    //     ip_query: (ip) =>
    //         from(getDocs(query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("ip", "==", ip)))).pipe(
    //             rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
    //             defaultIfEmpty([])
    //         ),

    //     users_query: (ids) =>
    //         from(getDocs(query(collection(db, "users"), where("ids", "array-contains-any", [...ids])))).pipe(
    //             rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
    //             defaultIfEmpty([])
    //         ),
    // },

    // user: {
    //     events: ({ email, lower_case_email, roas_user_id }) => {
    //         let func_name = `ClickFunnels:user:events`;
    //         console.log(func_name);

    //         if (!email) return throwError(`error:${func_name}:no email`);
    //         if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
    //         if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

    //         let users_docs = rxof([email, lower_case_email]).pipe(concatMap(Clickfunnels.queries.users_query));
    //         let user_ids = users_docs.pipe(
    //             rxmap(get(all, "ids")),
    //             rxmap(flatten),
    //             concatMap(Clickfunnels.queries.users_query),
    //             rxmap(get(all, "ids")),
    //             rxmap(flatten)
    //         );

    //         let email_ids = user_ids.pipe(
    //             concatMap(identity),
    //             rxfilter(isEmail),
    //             rxreduce((prev, curr) => [...prev, ...curr]),
    //             rxmap((ids) => [...ids, email, lower_case_email])
    //         );

    //         let ipv4_ids = user_ids.pipe(
    //             concatMap(identity),
    //             rxfilter(isIPv4),
    //             rxreduce((prev, curr) => [...prev, ...curr])
    //         );

    //         let ipv6_ids = user_ids.pipe(
    //             concatMap(identity),
    //             rxfilter(isIPv6),
    //             rxreduce((prev, curr) => [...prev, ...curr])
    //         );

    //         let email_docs = email_ids.pipe(
    //             concatMap(identity),
    //             concatMap(Clickfunnels.queries.email_query),
    //             rxreduce((prev, curr) => [...prev, ...curr]),
    //             rxmap(flatten)
    //         );

    //         let ipv4_docs = ipv4_ids.pipe(
    //             concatMap(identity),
    //             concatMap(Clickfunnels.queries.ip_query),
    //             rxreduce((prev, curr) => [...prev, ...curr]),
    //             rxmap(flatten)
    //         );

    //         let ipv6_docs = ipv6_ids.pipe(
    //             concatMap(identity),
    //             concatMap(Clickfunnels.queries.ip_query),
    //             rxreduce((prev, curr) => [...prev, ...curr]),
    //             rxmap(flatten)
    //         );

    //         return zip([email_docs]).pipe(rxmap(([one]) => [...one]));
    //     },
    queries: {
        email_query: curry((roas_user_id, email) => {
            let func_name = `ClickFunnels:queries:email_query`;
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            return from(getDocs(query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("email", "==", email)))).pipe(
                rxmap((snapshot) => snapshot.docs.map((doc) => doc.data()))
            );
        }),

        ip_query: curry((roas_user_id, ip) => {
            let func_name = `ClickFunnels:queries:ip_query`;
            console.log(func_name);

            if (!ip) return throwError(`error:${func_name}:no ip`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            return from(getDocs(query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("ip", "==", ip)))).pipe(
                rxmap((snapshot) => snapshot.docs.map((doc) => doc.data()))
            );
        }),

        user_query: curry((roas_user_id, id) => {
            let func_name = `ClickFunnels:queries:user_query`;
            console.log(func_name);

            let user_query = from(getDocs(query(collection(db, "users"), where("ids", "array-contains", id))));
            return user_query.pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
        }),

        users_query: curry((roas_user_id, ids) => {
            let func_name = `ClickFunnels:queries:users_query`;
            console.log(func_name);

            // console.log(roas_user_id);
            // console.log(ids);

            if (!ids) return throwError(`error:${func_name}:no ids`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            if (isEmpty(ids)) {
                return rxof([]);
            } else {
                return rxof(ids).pipe(
                    rxmap(uniq),
                    concatMap(identity),
                    concatMap(pipe(ClickFunnels.queries.user_query(roas_user_id))),
                    rxreduce((prev, curr) => [...prev, ...curr])
                );
            }
        }),
    },

    user: {
        events: ({ type, email, lower_case_email, roas_user_id }) => {
            // let func_name = `ClickFunnels:user:events`;
            // console.log(func_name);

            // if (!email) return throwError(`error:${func_name}:no email`);
            // if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
            // if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            // let case_insensitive_email_query = query(
            //     collection(db, "clickfunnels"),
            //     where("user_id", "==", roas_user_id),
            //     where("email", "==", lower_case_email)
            // );
            // let case_sensitive_email_query = query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("email", "==", email));

            // let case_insensitive_docs = from(getDocs(case_insensitive_email_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            // let case_sensitive_docs = from(getDocs(case_sensitive_email_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));

            // return zip([case_insensitive_docs, case_sensitive_docs]).pipe(rxmap(([insensitive, sensitive]) => [...insensitive, ...sensitive]));

            let func_name = `ClickFunnels:user:events`;
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            let users_docs = rxof([email, lower_case_email]).pipe(concatMap(pipe(ClickFunnels.queries.users_query(roas_user_id))));

            let user_ids = users_docs.pipe(
                rxmap(get(all, "ids")),
                rxmap(flatten),
                concatMap(pipe(ClickFunnels.queries.users_query(roas_user_id))),
                rxmap(get(all, "ids")),
                rxmap(flatten)
            );

            let email_ids = user_ids.pipe(
                concatMap(identity),
                rxfilter(isEmail),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap((ids) => [...ids, email, lower_case_email]),
                rxmap(uniq)
            );

            // return email_ids;

            let ipv4_ids = user_ids.pipe(
                concatMap(identity),
                rxfilter(isIPv4),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr])
            );

            let ipv6_ids = user_ids.pipe(
                concatMap(identity),
                rxfilter(isIPv6),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr])
            );

            // return ipv6_ids;

            let email_docs = email_ids.pipe(
                concatMap(identity),
                concatMap(pipe(ClickFunnels.queries.email_query(roas_user_id))),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(flatten)
            );

            // return email_docs;

            let ipv4_docs = ipv4_ids.pipe(
                concatMap(identity),
                concatMap(pipe(ClickFunnels.queries.ip_query(roas_user_id))),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(flatten)
            );

            let ipv6_docs = ipv6_ids.pipe(
                concatMap(identity),
                concatMap(pipe(ClickFunnels.queries.ip_query(roas_user_id))),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(flatten)
            );

            if (type == "ipv4") {
                return ipv4_docs;
            }

            if (type == "ipv6") {
                return ipv6_docs;
            }

            if ((type = "email")) {
                return email_docs;
            }

            return rxof([]);
        },

        ip: ({ type, email, lower_case_email, roas_user_id }) => {
            let func_name = `ClickFunnels:user:ip`;
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            return ClickFunnels.user
                .events({ type, email, lower_case_email, roas_user_id })
                .pipe(rxmap(mod(all)((event) => event.ip)), rxmap(uniq), rxmap(mod(all)((ip_address) => ({ ip_address, roas_user_id }))));
        },
    },
};

const Woocommerce = {
    user: {
        events: ({ email, lower_case_email, roas_user_id }) => {
            let func_name = `Woocommerce:user:events`;
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            let case_insensitive_email_query = query(
                collection(db, "clickfunnels"),
                where("user_id", "==", roas_user_id),
                where("email", "==", lower_case_email)
            );

            let case_sensitive_email_query = query(collection(db, "clickfunnels"), where("user_id", "==", roas_user_id), where("email", "==", email));

            let case_insensitive_docs = from(getDocs(case_insensitive_email_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            let case_sensitive_docs = from(getDocs(case_sensitive_email_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));

            return zip([case_insensitive_docs, case_sensitive_docs]).pipe(rxmap(([insensitive, sensitive]) => [...insensitive, ...sensitive]));
        },

        ip: ({ email, lower_case_email, roas_user_id }) => {
            let func_name = `Woocommerce:user:ip`;
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!lower_case_email) return throwError(`error:${func_name}:no lower_case_email`);
            if (!roas_user_id) return throwError(`error:${func_name}:no roas_user_id`);

            return Woocommerce.user
                .events({ email, lower_case_email, roas_user_id })
                .pipe(rxmap(mod(all)((event) => event.ip)), rxmap(uniq), rxmap(mod(all)((ip_address) => ({ ip_address, roas_user_id }))));
        },
    },
};

const rxreducer = rxreduce((prev, curr) => [...prev, ...curr]);

const ClickFunnelEvents = {
    fb_ad_id: {
        paths: (event) =>
            pipe(
                paths([
                    ["fb_ad_id"],
                    ["additional_info", "fb_ad_id"],
                    ["h_ad_id"],
                    ["additional_info", "h_ad_id"],
                    ["fb_id"],
                    ["additional_info", "fb_id"],
                ])
            )(event),
        get: (event) => pipe(ClickFunnelEvents.fb_ad_id.paths)(event),
    },

    ip: {
        paths: (event) => pipe(paths([["ip"], ["contact", "ip"]]))(event),
        get: (event) => pipe(ClickFunnelEvents.ip.paths)(event),
    },
};

const getQueryDocs = (snapshot) => (snapshot.docs ? snapshot.docs.map((doc) => ({ ...doc.data(), doc_id: doc.id })) : []);

const ipEvents = curry((version, ip) => {
    let q = query(collection(db, "events"), where(version, "==", ip));
    return from(getDocs(q)).pipe(rxmap(getQueryDocs));
});

const cfUser = curry((prop, value) => {
    let q = query(collection(db, "clickfunnels"), where(prop, "==", value));
    return from(getDocs(q)).pipe(rxmap(getQueryDocs));
});

const orders = ({ date, payment_processor_id, user_id }) => {
    let func_name = "Keap:orders";
    console.log(func_name);

    let since = `${moment(date, "YYYY-MM-DD").format("YYYY-MM-DD")}T08:00:00.000Z`;
    let until = `${moment(date, "YYYY-MM-DD").add(1, "days").format("YYYY-MM-DD")}T08:00:00.000Z`;

    return from(getDocs(query(collectionGroup(db, "integrations"), where("account_name", "==", "keap")), where("user_id", "==", user_id))).pipe(
        rxmap((data) => data.docs.map((doc) => doc.data())),
        rxmap(loorderby(["created_at"], ["desc"])),
        rxmap(head),
        concatMap((tokens) => {
            let { access_token } = tokens;

            let config = (access_token) => {
                return {
                    method: "get",
                    url: `https://api.infusionsoft.com/crm/rest/v1/orders?since=${since}&until=${until}`,
                    headers: {
                        Authorization: `Bearer ${access_token}`,
                    },
                };
            };

            if (access_token) {
                return from(axios(config(access_token))).pipe(rxmap(get("data", "orders")));
            } else {
                return rxof([]);
            }
        }),
        catchError((error) => {
            console.log("keap:orders:error");
            console.log(error.toJSON());
            return rxof([]);
        })
    );
};

const Hooks = {
    list: (user_id) => {
        let func_name = "Hooks:list";
        console.log(func_name);

        let access_token_docs = getDocs(
            query(collectionGroup(db, "integrations"), where("account_name", "==", "keap"), where("user_id", "==", user_id))
        );

        let config = (access_token) => {
            return {
                method: "get",
                url: `https://api.infusionsoft.com/crm/rest/v1/hooks/event_keys`,
                headers: {
                    Authorization: `Bearer ${access_token}`,
                },
            };
        };

        return from(access_token_docs).pipe(
            rxmap(Keap.utilities.queryDocs),
            rxmap(loorderby(["created_at"], ["desc"])),
            rxmap(head),
            concatMap((account) => {
                let { access_token } = account;
                return from(axios(config(access_token))).pipe(rxmap(get("data")));
            })
        );
    },

    subscribe: (hook_name, user_id) => {
        let func_name = "Hooks:subscribe";
        console.log(func_name);

        let access_token_docs = getDocs(
            query(collectionGroup(db, "integrations"), where("account_name", "==", "keap"), where("user_id", "==", user_id))
        );

        return from(access_token_docs).pipe(
            rxmap(Keap.utilities.queryDocs),
            rxmap(loorderby(["created_at"], ["desc"])),
            rxmap(head),
            concatMap((account) => {
                let { access_token } = account;
                const body = {
                    eventKey: hook_name,
                    roas_hook_url: "https://roas-webhooks-service.herokuapp.com/keap_webhook",
                    keap_hook_subscription_url: "https://api.infusionsoft.com/crm/rest/v1/hooks",
                    type: "subscribe",
                    access_token,
                    user_id,
                    hook_name,
                };

                return from(
                    fetch(`https://roas-webhooks-service.herokuapp.com/keap_webhook/${user_id}`, {
                        method: "post",
                        body: JSON.stringify(body),
                        headers: { "Content-Type": "application/json", Authorization: `Bearer ${access_token}` },
                    })
                );
            })
        );
    },

    verify: (user_id) => {
        let func_name = "Hooks:verify";
        console.log(func_name);

        let access_token_docs = getDocs(
            query(collectionGroup(db, "integrations"), where("account_name", "==", "keap"), where("user_id", "==", user_id))
        );

        let config = (access_token) => {
            return {
                method: "get",
                url: `https://api.infusionsoft.com/crm/rest/v1/hooks`,
                headers: {
                    Authorization: `Bearer ${access_token}`,
                },
            };
        };

        return from(access_token_docs).pipe(
            rxmap(Keap.utilities.queryDocs),
            rxmap(loorderby(["created_at"], ["desc"])),
            rxmap(head),
            concatMap((account) => {
                let { access_token } = account;

                return from(axios(config(access_token))).pipe(rxmap(get("data")));
            })
        );
    },
};

const Keap = {
    utilities: {
        getDates: (startDate, endDate) => {
            const dates = [];
            let currentDate = startDate;
            const addDays = function (days) {
                const date = new Date(this.valueOf());
                date.setDate(date.getDate() + days);
                return date;
            };
            while (currentDate <= endDate) {
                dates.push(currentDate);
                currentDate = addDays.call(currentDate, 1);
            }
            return dates;
        },

        get_dates_range_array: (since, until) => {
            let start_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(since);

            let end_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(until);

            const dates = pipe(
                ([start_date, end_date]) => Rules.utilities.getDates(start_date, end_date),
                mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
            )([start_date, end_date]);

            return dates;
        },

        date_pacific_time: (date, timezone = "America/Los_Angeles") => moment(date).tz(timezone),

        date_start_end_timestamps: (
            start = moment().format("YYYY-MM-DD"),
            end = moment().format("YYYY-MM-DD"),
            timezone = "America/Los_Angeles"
        ) => ({
            start: moment(Keap.utilities.date_pacific_time(start, timezone)).add(1, "days").startOf("day").valueOf(),
            end: moment(Keap.utilities.date_pacific_time(end, timezone)).add(1, "days").endOf("day").valueOf(),
        }),

        rxreducer: rxreduce((prev, curr) => [...prev, ...curr]),

        queryDocs: (snapshot) => (snapshot.docs ? snapshot.docs.map((doc) => ({ ...doc.data(), doc_id: doc.id })) : []),
    },

    integrations: {
        webhooks: {
            clickfunnels_webhook: ClickFunnels,
        },
    },

    orders: {
        get: ({ user_id, date, payment_processor_id }) => {
            let func_name = "Keap:orders:get";
            console.log(func_name);

            if (!user_id) return throwError(`error:${func_name}:no user_id`);
            if (!date) return throwError(`error:${func_name}:no date`);
            if (!payment_processor_id) return throwError(`error:${func_name}:no payment_processor_id`);

            return orders({ date, payment_processor_id, user_id }).pipe(
                rxmap(Keap.orders.paid),
                rxmap(mod(all)(({ contact, ...rest }) => ({ ...contact, ...rest }))),
                rxmap(mod(all)((order) => ({ ...order, lower_case_email: toLower(order.email) }))),
                rxmap(mod(all)((order) => ({ ...order, roas_user_id: user_id }))),
                rxmap(mod(all)(pick(["first_name", "last_name", "email", "roas_user_id", "order_items", "lower_case_email"]))),
                rxmap(mod(all, "order_items", all)(({ price, name }) => ({ price, name }))),
                defaultIfEmpty([])
            );
        },

        paid: (orders) => {
            let func_name = "Keap:order:paid:";
            console.log(func_name);

            return pipe(get(matching({ status: "PAID" })))(orders);
        },
    },

    order: {
        stats: {
            get: (cart) => {
                let func_name = "Keap:order:stats:get";
                console.log(func_name);

                return { roassales: size(cart), roasrevenue: pipe(get(all, "price"), sum)(cart) };
            },
        },

        ads: {
            ids: ({ type, order, shopping_cart_id }) => {
                let func_name = `Keap:order:ads:get`;
                console.log(func_name);

                if (!order) return throwError(`error:${func_name}:no order`);
                if (!shopping_cart_id) return throwError(`error:${func_name}:no shopping_cart_id`);

                let { email, lower_case_email, roas_user_id } = order;

                return Keap.integrations.webhooks[shopping_cart_id]["user"]["ip"]({ type, email, lower_case_email, roas_user_id }).pipe(
                    concatMap(identity),
                    concatMap(Keap.order.events.get),
                    concatMap(identity),
                    rxmap((event) => ({
                        ipv4: Event.ip.v4(event),
                        ipv6: Event.ip.v6(event),
                        ad_id: Event.ad.id(event),
                        user_agent: Event.user_agent(event),
                        timestamp: Math.trunc(Timestamp.toUTCDigit(Math.trunc(Event.get_utc_timestamp(event)))),
                    })),
                    rxmap(of),
                    rxreduce((prev, curr) => [...prev, ...curr]),
                    rxmap(lofilter((event) => !isUndefined(event.ad_id))),
                    rxfilter((ads) => !isEmpty(ads)),
                    rxmap(louniqby("ad_id")),
                    rxmap(loorderby(["timestamp"], ["desc"])),
                    defaultIfEmpty([])
                );
            },
        },

        events: {
            get: (order) => {
                let func_name = `Keap:order:events:get`;
                console.log(func_name);

                let ipv4_events = Events.user.get.ipv4({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                let ipv6_events = Events.user.get.ipv6({ ip: order.ip_address, roas_user_id: order.roas_user_id });
                return zip([ipv4_events, ipv6_events]).pipe(
                    rxmap(([ipv4, ipv6]) => [...ipv4, ...ipv6]),
                    defaultIfEmpty([])
                );
            },
        },
    },

    customer: {
        normalize: (orders) => {
            let func_name = "Keap:customer:normalize";
            console.log(func_name);

            let email = pipe(get(all, "email"), head)(orders);
            let lower_case_email = pipe(get(all, "lower_case_email"), head)(orders);
            let first_name = pipe(get(all, "first_name"), head)(orders);
            let last_name = pipe(get(all, "last_name"), head)(orders);
            let roas_user_id = pipe(get(all, "roas_user_id"), head)(orders);
            let line_items = pipe(get(all, "order_items"), flatten)(orders);

            let payload = {
                email,
                lower_case_email,
                first_name,
                last_name,
                roas_user_id,
                line_items,
            };

            return payload;
        },
    },

    report: {
        get: ({ user_id, date, fb_ad_account_id, payment_processor_id, shopping_cart_id }) => {
            let func_name = `Keap:report:get`;
            console.log(func_name);

            let orders = Keap.orders.get({ payment_processor_id, date, user_id }).pipe(
                rxmap(logroupby("lower_case_email")),
                rxmap(mod(all)(Keap.customer.normalize)),
                rxmap(values),
                concatMap(identity),
                rxmap(({ line_items, ...order }) => ({
                    ...order,
                    cart: line_items,
                    stats: Keap.order.stats.get(line_items),
                })),
                rxmap(of),
                rxreducer
            );

            let customers_from_cf = from(orders).pipe(
                rxmap(identity),
                concatMap(identity),
                concatMap((customer) =>
                    from(cfUser("email", customer.lower_case_email)).pipe(
                        concatMap(identity),
                        rxmap((cfevent) => ({
                            ad_id: pipe(
                                compact,
                                uniq,
                                reject((id) => id == "%7B%7Bad.id%7D%7D"),
                                head
                            )(ClickFunnelEvents.fb_ad_id.get(cfevent)),
                            timestamp: pipe(get("updated_at_unix_timestamp"))(cfevent),
                            ip: pipe(ClickFunnelEvents.ip.get, compact, uniq)(cfevent),
                        })),
                        rxmap(of),
                        rxreducer,
                        rxmap(loorderby(["timesamp"], ["desc"])),
                        rxmap(get(matching({ ad_id: (id) => !isEmpty(id) }))),
                        rxmap(louniqby("ad_id")),
                        rxmap(louniqby("timestamp")),
                        rxmap((ads) => ({ ...customer, ads })),
                        rxmap(of)
                    )
                ),
                rxreducer
            );

            customers_from_cf_with_ad_ids = customers_from_cf.pipe(rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))));
            customers_from_cf_without_ad_ids = customers_from_cf.pipe(rxmap(get(matching({ ads: (ads) => isEmpty(ads) }))));

            // return customers_from_cf_without_ad_ids;

            let customers_from_db_events = customers_from_cf_without_ad_ids.pipe(
                // rxmap(get(all, "email")),
                concatMap(identity),
                concatMap((customer) =>
                    from(cfUser("email", customer.lower_case_email)).pipe(
                        concatMap(identity),
                        rxmap(ClickFunnelEvents.ip.get),
                        rxreducer,
                        rxmap(pipe(compact, uniq)),
                        concatMap(identity),
                        concatMap((ip_address) => {
                            return zip([from(ipEvents("ipv4", ip_address)), from(ipEvents("ipv6", ip_address))]).pipe(
                                rxmap(flatten),
                                concatMap(identity),
                                rxmap(paths([["ipv4"], ["ipv6"]])),
                                rxmap((ips) => [...ips, ip_address]),
                                rxmap(uniq)
                            );
                        }),
                        rxreducer,
                        rxmap(uniq),
                        rxmap(compact),
                        concatMap(identity),
                        concatMap((ip_address) =>
                            zip([from(ipEvents("ipv4", ip_address)), from(ipEvents("ipv6", ip_address))]).pipe(
                                rxmap(flatten),
                                concatMap(identity),
                                rxmap((event) => ({
                                    ad_id: pipe(
                                        paths([["fb_ad_id"], ["h_ad_id"], ["fb_id"]]),
                                        compact,
                                        uniq,
                                        reject((id) => id == "%7B%7Bad.id%7D%7D"),
                                        head
                                    )(event),
                                    timestamp: pipe(Event.get_utc_timestamp)(event),
                                    ip: ip_address,
                                })),
                                rxmap(of),
                                rxreducer,
                                rxmap(loorderby(["timesamp"], ["desc"])),
                                rxmap(get(matching({ ad_id: (id) => !isEmpty(id) })))
                            )
                        ),
                        rxreducer,
                        rxmap(louniqby("ad_id")),
                        rxmap(louniqby("timestamp")),
                        rxmap((ads) => ({ ...customer, ads })),
                        rxmap(of)
                    )
                ),
                rxreducer
            );

            // return customers_from_db_events;

            let customers_from_db_events_with_ips = customers_from_db_events.pipe(rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))));
            let customers_from_db_events_without_ips = customers_from_db_events.pipe(rxmap(get(matching({ ads: (ads) => isEmpty(ads) }))));

            // return customers_from_db_events_without_ips;

            return zip([customers_from_cf_with_ad_ids, customers_from_db_events_with_ips, customers_from_db_events_without_ips]).pipe(
                rxmap(flattenDeep),
                concatMap(identity),
                concatMap((order) => {
                    let { ads, email } = order;
                    let ad_ids = pipe(mod(all)(pick(["ad_id"])))(ads);

                    let ad_details = Facebook.ads.details.get({ ad_ids, fb_ad_account_id, user_id, date }).pipe(
                        rxfilter((ad) => !isUndefined(ad.asset_id)),
                        rxmap((ad) => ({
                            ...ad,
                            email,
                            // ipv4: pipe(get(matching({ ad_id: ad.ad_id }), "ipv4"), head)(ads),
                            // ipv6: pipe(get(matching({ ad_id: ad.ad_id }), "ipv6"), head)(ads),
                            // user_agent: pipe(get(matching({ ad_id: ad.ad_id }), "user_agent"), head)(ads),
                            timestamp: pipe(get(matching({ ad_id: ad.ad_id }), "timestamp"), head)(ads),
                        })),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr]),
                        defaultIfEmpty([])
                    );

                    return from(ad_details).pipe(
                        rxmap((ads) => ({ ...order, ads, email })),
                        defaultIfEmpty({ ...order, ads: [], email })
                    );
                }),
                // rxmap(pipeLog),
                rxmap(pick(["email", "cart", "ads", "stats", "lower_case_email"])),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))),
                rxmap(pipe(logroupby("lower_case_email"), mod(all)(mergeAll))),
                // rxmap(size),
                // rxmap(pipeLog),
                // rxmap(lokeyby("lower_case_email")),
                rxmap((customers) => ({ customers })),
                rxmap((customers) => ({
                    ...customers,
                    date,
                    user_id,
                })),
                catchError((error) => {
                    console.log("Keap:report:get:error");
                    console.log(error);
                    return rxof(error);
                }),
                defaultIfEmpty({ date, customers: {}, user_id })
            );
        },
    },
};

let user_id = "ak192nawaMVAtj0oTIehjrEfi333";
let date = "2022-05-14";

// from(getDocs(query(collectionGroup(db, "project_accounts"), where("roas_user_id", "==", user_id))))
//     .pipe(
//         rxmap(Keap.utilities.queryDocs),
//         rxmap(lofilter((project) => project.shopping_cart_name !== undefined)),
//         rxmap(head),
//         concatMap((project) => {
//             return from(
//                 getDocs(query(collectionGroup(db, "integrations"), where("account_name", "==", "facebook"), where("user_id", "==", user_id)))
//             ).pipe(
//                 rxmap(Keap.utilities.queryDocs),
//                 rxmap(head),
//                 rxmap((facebook) => ({ ...facebook, ...project }))
//             );
//         })
//     )
//     .subscribe((project) => {
//         console.log("project");
//         console.log(project);

//         let { roas_user_id: user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id } = project;
//         let payload = { user_id, fb_ad_account_id, payment_processor_id, shopping_cart_id, date };

//         Keap.report.get(payload).subscribe((result) => {
//             console.log("result");
//             pipeLog(result);
//         });
//     });

const keap_webhook_subscriptions_array = ["subscription.add", "subscription.delete", "subscription.edit", "order.add", "order.edit", "order.delete"];

// Hooks.list(user_id).subscribe(pipeLog);
// Hooks.subscribe("subscription.delete", user_id).subscribe(pipeLog);
// Hooks.verify(user_id)
//     .pipe(rxmap(get(matching({ hookUrl: (url) => url.includes("roas") }))))
//     .subscribe(pipeLog);

exports.Keap = Keap;

// from(getDocs(query(collectionGroup(db, "integrations"), where("account_name", "==", "keap"))))
//     .pipe(
//         rxmap(queryDocs),
//         concatMap(identity),
//         concatMap((account) => {
//             return Hooks.verify(account.user_id).pipe(
//                 rxmap((values) => ({ success: account.user_id })),
//                 catchError((error) => rxof({ error: account.user_id }))
//             );
//         })
//     )
//     .subscribe((account) => {
//         console.log(account);
//     });
