package blueeyes.core.service

import blueeyes.core.http._
import blueeyes.util.printer._

import org.specs.Specification
import blueeyes.core.service.RestPathPatternParsers.LiteralPathPattern

class MetadataFormatterSpec extends Specification {

  "html service metadata" should {
    "print a simple set of metadata in html format" in {
      val expected = """<html>

  <head>
      <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
      <style>
        body         {line-height: 1;font: 13px/1.5 'Helvetica Neue',Arial,'Liberation Sans',FreeSans,sans-serif;color: #555;}
        td           {padding: 6px 0 6px 0;vertical-align: top;}
        table        {width:100%}
        div.nested   {padding-bottom: 10px; padding-top: 10px; padding-left: 50px; border-bottom: 1px solid #EEE;}
        div.root     {padding-left: 50px;padding-top: 10px;padding-right: 50px;}
        td.types     {width: 15%;}
        td.values    {width: 25%}
        td.desc      {width: 60%;}
        .font-values {color: #0094C2;}
        .font-types, .caption {font-weight: bold;}
        h1, .caption {color: #333;}
        h1           {font-size: 3em;font-weight: 300;margin: 0;padding: 0;}
        div.desc     {color: #777;margin-top: 0.5em}
      </style>
<title>REST API Resources</title>  </head>

  <body>
    <div class = "root">
    <h1>REST API Resources</h1>
    <table>
      <tbody>
        <tr><td>Timelines are collections of Tweets, ordered with the most recent first.</td></tr>
      </tbody>
    </table>


<div class="nested">
        <table>
          <tbody>
            <tr>
                <td class="types caption">Parameter Type</td>
                <td class="values caption">Parameter</td>
                <td class="desc caption">Description</td>
            </tr>
          </tbody>
        </table></div>
<div class="nested">
        <table>
          <tbody>
            <tr>
                <td class="types font-types">Service path</td>
                <td class="values font-values">/statuses/home_timeline.format</td>
                <td class="desc">Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">HTTP method</td>
                <td class="values font-values">GET</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Request Parameter</td>
                <td class="values font-values">'callback</td>
                <td class="desc">A callback method identifier is required when using JsonP with a "GET" request. (required)</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Supported encodings</td>
                <td class="values font-values">gzip, deflate</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table></div>

<div class="nested">
        <table>
          <tbody>
            <tr>
                <td class="types font-types">Service path</td>
                <td class="values font-values">/statuses/home_timeline.format</td>
                <td class="desc">Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">HTTP method</td>
                <td class="values font-values">GET</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Request Parameter</td>
                <td class="values font-values">'callback</td>
                <td class="desc">A callback method identifier is required when using JsonP with a "GET" request. (required)</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Supported encodings</td>
                <td class="values font-values">gzip, deflate</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table></div>

<div class="nested">
        <table>
          <tbody>
            <tr>
                <td class="types font-types">Service path</td>
                <td class="values font-values">/statuses/home_timeline.format</td>
                <td class="desc">Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">HTTP method</td>
                <td class="values font-values">GET</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Request Parameter</td>
                <td class="values font-values">'callback</td>
                <td class="desc">A callback method identifier is required when using JsonP with a "GET" request. (required)</td>
            </tr>
          </tbody>
        </table>

        <table>
          <tbody>
            <tr>
                <td class="types font-types">Supported encodings</td>
                <td class="values font-values">gzip, deflate</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table></div>

<div class="nested">
        <table>
          <tbody>
            <tr>
                <td class="types font-types">HTTP method</td>
                <td class="values font-values">DELETE</td>
                <td class="desc"></td>
            </tr>
          </tbody>
        </table></div>
     </div>
  </body>
</html>"""
      HtmlPrinter.printFormatted(AndMetadata(
        DescriptionMetadata("Timelines are collections of Tweets, ordered with the most recent first."),
        OrMetadata(
        AndMetadata(
          HttpMethodMetadata(HttpMethods.GET),
          PathPatternMetadata(LiteralPathPattern("/statuses/home_timeline.format"), Some("""Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...""")),
          EncodingMetadata(Encodings.gzip, Encodings.deflate),
          ParameterMetadata('callback, None, Some("A callback method identifier is required when using JsonP with a \"GET\" request."))
        ),
          AndMetadata(
            HttpMethodMetadata(HttpMethods.GET),
            PathPatternMetadata(LiteralPathPattern("/statuses/home_timeline.format"), Some("""Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...""")),
            EncodingMetadata(Encodings.gzip, Encodings.deflate),
            ParameterMetadata('callback, None, Some("A callback method identifier is required when using JsonP with a \"GET\" request."))
          ),
          AndMetadata(
            HttpMethodMetadata(HttpMethods.GET),
            PathPatternMetadata(LiteralPathPattern("/statuses/home_timeline.format"), Some("""Returns the 20 most recent statuses, including retweets if they exist, posted by the authenticating user and the user's they follow. This is the same timeline seen by a user when they login to twitter.com. This method is identical to statuses/friends_timeline, except that this method always...""")),
            EncodingMetadata(Encodings.gzip, Encodings.deflate),
            ParameterMetadata('callback, None, Some("A callback method identifier is required when using JsonP with a \"GET\" request."))
          ),
        HttpMethodMetadata(HttpMethods.DELETE)
      ))) must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
