"""Main CLI entrypoint for TRC."""
import click
from .commands import auth, user, ls, version


@click.group()
def main():
    """Trossen Robotics Cloud CLI."""
    pass


# Register command groups and commands
main.add_command(auth.auth)
main.add_command(user.user)
main.add_command(ls.ls)
main.add_command(version.version)


if __name__ == '__main__':
    main()
